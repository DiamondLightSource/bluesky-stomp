import itertools
import logging
import time
import uuid
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from threading import Event
from typing import Any, TypeVar, cast

from stomp.connect import ConnectionListener  # type: ignore
from stomp.connect import StompConnection11 as Connection  # type: ignore
from stomp.exception import ConnectFailedException  # type: ignore
from stomp.utils import Frame  # type: ignore

from .models import (
    BasicAuthentication,
    Broker,
    DestinationBase,
    MessageQueue,
    MessageTopic,
    TemporaryMessageQueue,
)
from .serdes import (
    MessageDeserializer,
    MessageSerializer,
    deserialize_message,
    determine_callback_deserialization_type,
    serialize_message,
)
from .utils import handle_all_exceptions

CORRELATION_ID_HEADER = "correlation-id"

T = TypeVar("T")


@dataclass
class MessageContext:
    """
    Context that comes with a message, provides useful information such as how to reply
    """

    destination: DestinationBase
    reply_destination: DestinationBase | None
    correlation_id: str | None


MessageListener = Callable[[Any, MessageContext], None]
ErrorListener = Callable[[Exception], None]


@dataclass
class StompReconnectPolicy:
    """
    Details of how often stomp will try to reconnect if connection is unexpectedly lost
    """

    initial_delay: float = 0.0
    attempt_period: float = 10.0


@dataclass
class Subscription:
    """
    Details of a subscription, the template needs its own representation to
    defer subscriptions until after connection
    """

    destination: DestinationBase
    callback: Callable[[Frame], None]
    on_error: ErrorListener | None


class MessagingTemplate:
    """
    MessagingTemplate that uses the stomp protocol, meant for use
    with ActiveMQ.
    """

    def __init__(
        self,
        conn: Connection,
        reconnect_policy: StompReconnectPolicy | None = None,
        authentication: BasicAuthentication | None = None,
        serializer: MessageSerializer = serialize_message,
        deserializer: MessageDeserializer[Any] = deserialize_message,
    ) -> None:
        self._conn = conn
        self._reconnect_policy = reconnect_policy or StompReconnectPolicy()
        self._authentication = authentication or None
        self._serializer = serializer
        self._deserializer = deserializer

        self._sub_num = itertools.count()
        self._listener = ConnectionListener()

        self._listener.on_message = self._on_message
        self._conn.set_listener("", self._listener)  # type: ignore

        self._subscriptions: dict[str, Subscription] = {}

    @classmethod
    def for_broker(cls, broker: Broker) -> "MessagingTemplate":
        return cls(
            Connection(
                [(broker.host, broker.port)],
                auto_content_length=False,
            ),
            authentication=broker.auth,
        )

    def send_and_receive(
        self,
        destination: DestinationBase,
        obj: Any,
        reply_type: type[T] = str,
        correlation_id: str | None = None,
    ) -> Future[T]:
        """
        Send a message expecting a single reply.

        Args:
            destination (str): Destination to send the message
            obj (Any): Message to send, must be serializable
            reply_type (Type, optional): Expected type of reply, used
                                         in deserialization. Defaults to str.
            correlation_id (Optional[str]): An id which correlates this request with
                                                requests it spawns or the request which
                                                spawned it etc.
        Returns:
            Future: Future representing the reply
        """

        future: Future[T] = Future()

        def callback(reply: T, _: MessageContext) -> None:
            future.set_result(reply)

        callback.__annotations__["reply"] = reply_type
        self.send(
            destination,
            obj,
            on_reply=callback,
            on_reply_error=future.set_exception,
            correlation_id=correlation_id,
        )
        return future

    def send(
        self,
        destination: DestinationBase,
        obj: Any,
        on_reply: MessageListener | None = None,
        on_reply_error: ErrorListener | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raw_destination = _destination(destination)
        serialized_message = self._serializer(obj)
        self._send_str(
            raw_destination,
            serialized_message,
            on_reply,
            on_reply_error,
            correlation_id,
        )

    def _send_str(
        self,
        destination: str,
        message: str,
        on_reply: MessageListener | None = None,
        on_reply_error: ErrorListener | None = None,
        correlation_id: str | None = None,
    ) -> None:
        logging.info(f"SENDING {message} to {destination}")

        headers: dict[str, Any] = {"JMSType": "TextMessage"}
        if on_reply is not None:
            reply_queue = TemporaryMessageQueue(name=str(uuid.uuid1()))
            headers = {**headers, "reply-to": _destination(reply_queue)}
            self.subscribe(
                reply_queue,
                on_reply,
                on_error=on_reply_error,
            )
        if correlation_id:
            headers = {**headers, CORRELATION_ID_HEADER: correlation_id}
        self._conn.send(headers=headers, body=message, destination=destination)  # type: ignore

    def listener(
        self,
        destination: DestinationBase,
        on_error: ErrorListener | None = None,
    ):
        """
        Decorator for subscribing to a MessageTopic:

        @my_app.listener("my-destination")
        def callback(context: MessageContext, message: ???) -> None:
            ...

        Args:
            destination (str): Destination to subscribe to
        """

        def decorator(callback: MessageListener) -> MessageListener:
            self.subscribe(
                destination,
                callback,
                on_error=on_error,
            )
            return callback

        return decorator

    def subscribe(
        self,
        destination: DestinationBase,
        callback: MessageListener,
        on_error: ErrorListener | None = None,
    ) -> None:
        logging.debug(f"New subscription to {destination}")
        obj_type = determine_callback_deserialization_type(callback)

        def wrapper(frame: Frame) -> None:
            headers = frame.headers  # type: ignore
            headers = cast(Mapping[str, str], headers)
            body = frame.body  # type: ignore
            body = cast(str | bytes | bytearray, body)

            value = self._deserializer(body, obj_type)

            reply_to: str | None = headers.get("reply-to")
            context = MessageContext(
                _destination_from_str(headers["destination"]),
                _destination_from_str(reply_to) if reply_to is not None else None,
                headers.get(CORRELATION_ID_HEADER),
            )

            callback(value, context)

        sub_id = (
            _destination(destination)
            if isinstance(destination, TemporaryMessageQueue)
            else str(next(self._sub_num))
        )
        self._subscriptions[sub_id] = Subscription(
            destination, wrapper, on_error=on_error
        )
        # If we're connected, subscribe immediately, otherwise the subscription is
        # deferred until connection.
        self._ensure_subscribed([sub_id])

    def connect(self) -> None:
        if self._conn.is_connected():
            return

        connected: Event = Event()

        def finished_connecting(frame: Frame) -> None:
            connected.set()

        self._listener.on_connected = finished_connecting
        self._listener.on_disconnected = self._on_disconnected

        logging.info("Connecting...")

        try:
            if self._authentication is not None:
                self._conn.connect(  # type: ignore
                    username=self._authentication.username,
                    passcode=self._authentication.password,
                    wait=True,
                )
            else:
                self._conn.connect(wait=True)  # type: ignore
            connected.wait()
        except ConnectFailedException as ex:
            logging.exception(msg="Failed to connect to message bus", exc_info=ex)

        self._ensure_subscribed()

    def _ensure_subscribed(self, sub_ids: list[str] | None = None) -> None:
        # We must defer subscription until after connection, because stomp literally
        # sends a SUB to the broker. But it still nice to be able to call subscribe
        # on template before it connects, then just run the subscribes after connection.
        if self._conn.is_connected():
            for sub_id in sub_ids or self._subscriptions.keys():
                sub = self._subscriptions[sub_id]
                logging.info(f"Subscribing to {sub.destination}")
                raw_destination = _destination(sub.destination)
                self._conn.subscribe(  # type: ignore
                    destination=raw_destination,
                    id=sub_id,
                    ack="auto",
                )

    def disconnect(self) -> None:
        logging.info("Disconnecting...")
        if not self.is_connected():
            logging.info("Already disconnected")
            return
        # We need to synchronise the disconnect on an event because the stomp Connection
        # object doesn't do it for us
        disconnected = Event()
        self._listener.on_disconnected = disconnected.set
        self._conn.disconnect()  # type: ignore
        disconnected.wait()

    @handle_all_exceptions
    def _on_disconnected(self) -> None:
        logging.warning(
            "Stomp connection lost, will attempt reconnection with "
            f"policy {self._reconnect_policy}"
        )
        time.sleep(self._reconnect_policy.initial_delay)
        while not self._conn.is_connected():
            try:
                self.connect()
            except ConnectFailedException:
                logging.exception("Reconnect failed")
            time.sleep(self._reconnect_policy.attempt_period)

    @handle_all_exceptions
    def _on_message(self, frame: Frame) -> None:
        logging.info(f"Received {frame}")
        headers: Mapping[str, Any] = cast(
            Mapping[str, Any],
            frame.headers,  # type: ignore
        )
        if (sub_id := headers.get("subscription")) is not None:
            if (sub := self._subscriptions.get(sub_id)) is not None:
                try:
                    sub.callback(frame)
                except Exception as ex:
                    if sub.on_error is not None:
                        sub.on_error(ex)
                    else:
                        raise ex
            else:
                logging.warning(f"No subscription active for id: {sub_id}")
        else:
            logging.warning(f"No subscription ID in message headers: {headers}")

    def is_connected(self) -> bool:
        return self._conn.is_connected()  # type: ignore


def _destination(destination: DestinationBase) -> str:
    match destination:
        case MessageQueue(name=name):
            return f"/queue/{name}"
        case TemporaryMessageQueue(name=name):
            return f"/temp-queue/{name}"
        case MessageTopic(name=name):
            return f"/topic/{name}"
        case _:
            raise TypeError(f"Unrecognized destination type: {destination}")


def _destination_from_str(destination: str) -> DestinationBase:
    if destination.startswith("/"):
        _, destination_type, destination_name = destination.split("/")
        constructor = {
            "queue": MessageQueue,
            "temp-queue": TemporaryMessageQueue,
            "topic": MessageTopic,
        }[destination_type]
        return constructor(name=destination_name)
    else:
        return MessageQueue(name=destination)
