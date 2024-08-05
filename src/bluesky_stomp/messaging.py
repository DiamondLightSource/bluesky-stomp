import inspect
import itertools
import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event
from typing import Any, Mapping

import stomp
from pydantic import BaseModel, Field, TypeAdapter
from stomp.exception import ConnectFailedException
from stomp.utils import Frame

from .models import (AuthenticationBase, DestinationBase, Queue,
                     TemporaryQueue, Topic)
from .utils import handle_all_exceptions

LOGGER = logging.getLogger(__name__)

CORRELATION_ID_HEADER = "correlation-id"


@dataclass
class MessageContext:
    """
    Context that comes with a message, provides useful information such as how to reply
    """

    destination: str
    reply_destination: str | None
    correlation_id: str | None


MessageListener = Callable[[Any], None]
ContextualMessageListener = Callable[[MessageContext, Any], None]


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


class MessagingTemplate:
    """
    MessagingTemplate that uses the stomp protocol, meant for use
    with ActiveMQ.
    """

    def __init__(
        self,
        conn: stomp.Connection,
        reconnect_policy: StompReconnectPolicy | None = None,
        authentication: AuthenticationBase | None = None,
    ) -> None:
        self._conn = conn
        self._reconnect_policy = reconnect_policy or StompReconnectPolicy()
        self._authentication = authentication or None

        self._sub_num = itertools.count()
        self._listener = stomp.ConnectionListener()

        self._listener.on_message = self._on_message
        self._conn.set_listener("", self._listener)

        self._subscriptions = {}

    @classmethod
    def for_host_and_port(
        cls,
        host: str,
        port: int,
        auth: AuthenticationBase | None = None,
    ) -> "MessagingTemplate":
        return cls(
            stomp.Connection(
                [(host, port)],
                auto_content_length=False,
            ),
            authentication=auth,
        )

    @classmethod
    def localhost(
        cls,
        auth: AuthenticationBase | None = None,
    ) -> "MessagingTemplate":
        return cls.for_host_and_port("localhost", 61613, auth=auth)

    def send(
        self,
        destination: DestinationBase,
        obj: Any,
        on_reply: MessageListener | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raw_destination = _destination(destination)
        serialized_message = json.dumps(_serialize(obj))
        self._send_str(
            raw_destination,
            serialized_message,
            on_reply,
            correlation_id,
        )

    def _send_str(
        self,
        destination: str,
        message: str,
        on_reply: MessageListener | None = None,
        correlation_id: str | None = None,
    ) -> None:
        LOGGER.info(f"SENDING {message} to {destination}")

        headers: dict[str, Any] = {"JMSType": "TextMessage"}
        if on_reply is not None:
            reply_queue_name = self.destinations.temporary_queue(str(uuid.uuid1()))
            headers = {**headers, "reply-to": reply_queue_name}
            self.subscribe(reply_queue_name, on_reply)
        if correlation_id:
            headers = {**headers, CORRELATION_ID_HEADER: correlation_id}
        self._conn.send(headers=headers, body=message, destination=destination)

    def subscribe(
        self, destination: DestinationBase, callback: MessageListener | ContextualMessageListener,
    ) -> None:
        LOGGER.debug(f"New subscription to {destination}")
        obj_type = determine_deserialization_type(callback, default=str)

        def wrapper(frame: Frame) -> None:
            as_dict = json.loads(frame.body)
            adapter = TypeAdapter(obj_type)
            value: Any = adapter.validate_python(as_dict)

            context = MessageContext(
                frame.headers["destination"],
                frame.headers.get("reply-to"),
                frame.headers.get(CORRELATION_ID_HEADER),
            )
            
            callback(value, context)

        sub_id = (
            destination
            if isinstance(destination, TemporaryQueue)
            else str(next(self._sub_num))
        )
        self._subscriptions[sub_id] = Subscription(destination, wrapper)
        # If we're connected, subscribe immediately, otherwise the subscription is
        # deferred until connection.
        self._ensure_subscribed([sub_id])

    def connect(self) -> None:
        if self._conn.is_connected():
            return

        connected: Event = Event()

        def finished_connecting(_: Frame):
            connected.set()

        self._listener.on_connected = finished_connecting
        self._listener.on_disconnected = self._on_disconnected

        LOGGER.info("Connecting...")

        try:
            if self._authentication is not None:
                self._conn.connect(
                    username=self._authentication.username,
                    passcode=self._authentication.passcode,
                    wait=True,
                )
            else:
                self._conn.connect(wait=True)
            connected.wait()
        except ConnectFailedException as ex:
            LOGGER.exception(msg="Failed to connect to message bus", exc_info=ex)

        self._ensure_subscribed()

    def _ensure_subscribed(self, sub_ids: list[str] | None = None) -> None:
        # We must defer subscription until after connection, because stomp literally
        # sends a SUB to the broker. But it still nice to be able to call subscribe
        # on template before it connects, then just run the subscribes after connection.
        if self._conn.is_connected():
            for sub_id in sub_ids or self._subscriptions.keys():
                sub = self._subscriptions[sub_id]
                LOGGER.info(f"Subscribing to {sub.destination}")
                raw_destination = _destination(sub.destination)
                self._conn.subscribe(destination=raw_destination, id=sub_id, ack="auto")

    def disconnect(self) -> None:
        LOGGER.info("Disconnecting...")
        if not self.is_connected():
            LOGGER.info("Already disconnected")
            return
        # We need to synchronise the disconnect on an event because the stomp Connection
        # object doesn't do it for us
        disconnected = Event()
        self._listener.on_disconnected = disconnected.set
        self._conn.disconnect()
        disconnected.wait()
        self._listener.on_disconnected = None

    @handle_all_exceptions
    def _on_disconnected(self) -> None:
        LOGGER.warn(
            "Stomp connection lost, will attempt reconnection with "
            f"policy {self._reconnect_policy}"
        )
        time.sleep(self._reconnect_policy.initial_delay)
        while not self._conn.is_connected():
            try:
                self.connect()
            except ConnectFailedException:
                LOGGER.exception("Reconnect failed")
            time.sleep(self._reconnect_policy.attempt_period)

    @handle_all_exceptions
    def _on_message(self, frame: Frame) -> None:
        LOGGER.info(f"Received {frame}")
        sub_id = frame.headers.get("subscription")
        sub = self._subscriptions.get(sub_id)
        if sub is not None:
            sub.callback(frame)
        else:
            LOGGER.warn(f"No subscription active for id: {sub_id}")

    def is_connected(self) -> bool:
        return self._conn.is_connected()


def _destination(destination: DestinationBase) -> str:
    match destination:
        case Queue(name=name):
            return f"/queue/{name}"
        case TemporaryQueue(name=name):
            return f"/temp-queue/{name}"
        case Topic(name=name):
            return f"/topic/{name}"
        case _:
            raise TypeError(f"Unrecognized destination type: {destination}")


def _serialize(obj: Any) -> Any:
    """
    Pydantic-aware serialization routine that can also be
    used on primitives. So serialize(4) is 4, but
    serialize(<model>) is a dictionary.

    Args:
        obj: The object to serialize

    Returns:
        Any: The serialized object
    """

    if isinstance(obj, BaseModel):
        # Serialize by alias so that our camelCase models leave the service
        # with camelCase field names
        return obj.model_dump(by_alias=True)
    else:
        return obj


def determine_deserialization_type(
    listener: MessageListener, default: type = str
) -> type:
    """
    Inspect a message listener function to determine the type to deserialize
    a message to

    Args:
        listener (MessageListener): The function that takes a deserialized message
        default (Type, optional): If the type cannot be determined, what default
                                  should we fall back on? Defaults to str.

    Returns:
        Type: _description_
    """

    _, message = inspect.signature(listener).parameters.values()
    a_type = message.annotation
    if a_type is not inspect.Parameter.empty:
        return a_type
    else:
        return default
