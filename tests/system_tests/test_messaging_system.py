import itertools
import logging
from collections.abc import Iterable
from concurrent.futures import Future
from queue import Queue
from typing import Any, cast

import numpy as np
import pytest
from pydantic import BaseModel
from stomp.exception import (  # type: ignore
    NotConnectedException,
)

from bluesky_stomp.messaging import MessageContext, StompClient
from bluesky_stomp.models import Broker, DestinationBase, MessageQueue, MessageTopic

_TIMEOUT: float = 10.0
_COUNT = itertools.count()


@pytest.fixture
def disconnected_client() -> StompClient:
    client = StompClient.for_broker(Broker.localhost())
    assert client is not None
    return client


@pytest.fixture
def client() -> Iterable[StompClient]:
    client = StompClient.for_broker(Broker.localhost())
    assert client is not None
    client.connect()
    yield client
    client.disconnect()


@pytest.fixture
def test_queue() -> MessageQueue:
    return MessageQueue(name=f"test-{next(_COUNT)}")


@pytest.fixture
def test_queue_2() -> MessageQueue:
    return MessageQueue(name=f"test-{next(_COUNT)}")


@pytest.fixture
def test_topic() -> MessageTopic:
    return MessageTopic(name=f"test-{next(_COUNT)}")


def test_disconnected_error(
    client: StompClient,
    test_queue: MessageQueue,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    acknowledge(client, test_queue)

    f: Future[str] = Future()

    def callback(message: str, ctx: MessageContext) -> None:
        f.set_result(message)

    if client.is_connected():
        client.disconnect()
    with pytest.raises(NotConnectedException):
        client.send(test_queue, "test_message", callback)

    client.disconnect()
    assert not client.is_connected()
    assert "Disconnecting..." in caplog.text
    assert "Already disconnected" in caplog.text


def test_send(client: StompClient, test_queue: MessageQueue) -> None:
    f: Future[str] = Future()

    def callback(message: str, ctx: MessageContext) -> None:
        f.set_result(message)

    client.subscribe(test_queue, callback, on_error=f.set_exception)
    client.send(test_queue, "test_message")
    assert f.result(timeout=_TIMEOUT) == "test_message"


def test_send_to_topic(client: StompClient, test_topic: MessageTopic) -> None:
    f: Future[str] = Future()

    def callback(message: str, ctx: MessageContext) -> None:
        f.set_result(message)

    client.subscribe(test_topic, callback, on_error=f.set_exception)
    client.send(test_topic, "test_message")
    assert f.result(timeout=_TIMEOUT) == "test_message"


def test_send_on_reply(client: StompClient, test_queue: MessageQueue) -> None:
    acknowledge(client, test_queue)

    f: Future[str] = Future()

    def callback(message: str, ctx: MessageContext) -> None:
        f.set_result(message)

    client.send(
        test_queue,
        "test_message",
        on_reply=callback,
        on_reply_error=f.set_exception,
    )
    assert f.result(timeout=_TIMEOUT) == "ack"


def test_send_and_receive(client: StompClient, test_queue: MessageQueue) -> None:
    acknowledge(client, test_queue)
    reply = client.send_and_receive(test_queue, "test", str).result(timeout=_TIMEOUT)
    assert reply == "ack"


def test_listener(client: StompClient, test_queue: MessageQueue) -> None:
    ack: Future[str] = Future()

    @client.listener(test_queue, on_error=ack.set_exception)
    def server(message: str, ctx: MessageContext) -> None:  # type: ignore
        reply_queue = ctx.reply_destination
        if reply_queue is None:
            raise RuntimeError("reply queue is None")
        client.send(reply_queue, "ack", correlation_id=ctx.correlation_id)
        ack.set_result("ack")

    reply_future = client.send_and_receive(test_queue, "test", str)
    assert ack.result(timeout=_TIMEOUT) == "ack"
    assert reply_future.result(timeout=_TIMEOUT) == "ack"


class Foo(BaseModel):
    a: int
    b: str


@pytest.mark.parametrize(
    "message,message_type",
    [
        ("test", str),
        (1, int),
        (Foo(a=1, b="test"), Foo),
        (np.array([1, 2, 3]), list),
    ],
)
def test_deserialization(
    client: StompClient,
    test_queue: MessageQueue,
    message: Any,
    message_type: type,
) -> None:
    ack: Future[message_type] = Future()  # type: ignore

    def server(message: message_type, ctx: MessageContext) -> None:  # type: ignore
        reply_queue = ctx.reply_destination
        if reply_queue is None:
            raise RuntimeError("reply queue is None")
        client.send(reply_queue, message, correlation_id=ctx.correlation_id)
        ack.set_result(message)  # type: ignore

    client.subscribe(test_queue, server, on_error=ack.set_exception)  # type: ignore

    reply_future: Future[message_type] = client.send_and_receive(  # type: ignore
        test_queue, message, message_type
    )
    result = ack.result(timeout=_TIMEOUT)  # type: ignore
    if isinstance(message, np.ndarray):
        message = cast(np.ndarray[Any, Any], message)
        message = message.tolist()
    assert result == message
    assert reply_future.result(timeout=_TIMEOUT) == message


def test_subscribe_before_connect(
    disconnected_client: StompClient, test_queue: MessageQueue
) -> None:
    acknowledge(disconnected_client, test_queue)
    disconnected_client.connect()
    reply = disconnected_client.send_and_receive(test_queue, "test", str).result(
        timeout=_TIMEOUT
    )
    assert reply == "ack"


def test_reconnect(client: StompClient, test_queue: MessageQueue) -> None:
    acknowledge(client, test_queue)
    reply = client.send_and_receive(test_queue, "test", str).result(timeout=_TIMEOUT)
    assert reply == "ack"
    client.disconnect()
    assert not client.is_connected()
    client.connect()
    assert client.is_connected()
    reply = client.send_and_receive(test_queue, "test", str).result(timeout=_TIMEOUT)
    assert reply == "ack"


def test_correlation_id(
    client: StompClient, test_queue: MessageQueue, test_queue_2: MessageQueue
) -> None:
    correlation_id = "foobar"
    q: Queue[MessageContext] = Queue()

    def server(msg: str, ctx: MessageContext) -> None:
        q.put(ctx)
        client.send(test_queue_2, msg, correlation_id=ctx.correlation_id)

    def callback(msg: str, ctx: MessageContext) -> None:
        q.put(ctx)

    client.subscribe(test_queue, server)
    client.subscribe(test_queue_2, callback)
    client.send(test_queue, "test", correlation_id=correlation_id)

    ctx_req: MessageContext = q.get(timeout=_TIMEOUT)
    assert ctx_req.correlation_id == correlation_id
    ctx_ack: MessageContext = q.get(timeout=_TIMEOUT)
    assert ctx_ack.correlation_id == correlation_id


def acknowledge(client: StompClient, destination: DestinationBase) -> None:
    def server(message: str, ctx: MessageContext) -> None:
        reply_queue = ctx.reply_destination
        if reply_queue is None:
            raise RuntimeError("reply queue is None")
        client.send(reply_queue, "ack", correlation_id=ctx.correlation_id)

    client.subscribe(destination, server)
