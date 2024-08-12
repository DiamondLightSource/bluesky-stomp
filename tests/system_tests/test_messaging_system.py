import itertools
import logging
from collections.abc import Iterable
from concurrent.futures import Future
from queue import Queue
from typing import Any

import numpy as np
import pytest
from pydantic import BaseModel
from stomp.exception import (  # type: ignore
    NotConnectedException,
)

from bluesky_stomp.messaging import MessageContext, MessagingTemplate
from bluesky_stomp.models import Broker, DestinationBase, MessageQueue, MessageTopic

_TIMEOUT: float = 10.0
_COUNT = itertools.count()


@pytest.fixture
def disconnected_template() -> MessagingTemplate:
    template = MessagingTemplate.for_broker(Broker.localhost())
    assert template is not None
    return template


@pytest.fixture
def template() -> Iterable[MessagingTemplate]:
    template = MessagingTemplate.for_broker(Broker.localhost())
    assert template is not None
    template.connect()
    yield template
    template.disconnect()


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
    template: MessagingTemplate,
    test_queue: MessageQueue,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    acknowledge(template, test_queue)

    f: Future[str] = Future()

    def callback(message: str, ctx: MessageContext) -> None:
        f.set_result(message)

    if template.is_connected():
        template.disconnect()
    with pytest.raises(NotConnectedException):
        template.send(test_queue, "test_message", callback)

    template.disconnect()
    assert not template.is_connected()
    assert "Disconnecting..." in caplog.text
    assert "Already disconnected" in caplog.text


def test_send(template: MessagingTemplate, test_queue: MessageQueue) -> None:
    f: Future[str] = Future()

    def callback(message: str, ctx: MessageContext) -> None:
        f.set_result(message)

    template.subscribe(test_queue, callback, on_error=f.set_exception)
    template.send(test_queue, "test_message")
    assert f.result(timeout=_TIMEOUT) == "test_message"


def test_send_to_topic(template: MessagingTemplate, test_topic: MessageTopic) -> None:
    f: Future[str] = Future()

    def callback(message: str, ctx: MessageContext) -> None:
        f.set_result(message)

    template.subscribe(test_topic, callback, on_error=f.set_exception)
    template.send(test_topic, "test_message")
    assert f.result(timeout=_TIMEOUT) == "test_message"


def test_send_on_reply(template: MessagingTemplate, test_queue: MessageQueue) -> None:
    acknowledge(template, test_queue)

    f: Future[str] = Future()

    def callback(message: str, ctx: MessageContext) -> None:
        f.set_result(message)

    template.send(
        test_queue,
        "test_message",
        on_reply=callback,
        on_reply_error=f.set_exception,
    )
    assert f.result(timeout=_TIMEOUT) == "ack"


def test_send_and_receive(
    template: MessagingTemplate, test_queue: MessageQueue
) -> None:
    acknowledge(template, test_queue)
    reply = template.send_and_receive(test_queue, "test", str).result(timeout=_TIMEOUT)
    assert reply == "ack"


def test_listener(template: MessagingTemplate, test_queue: MessageQueue) -> None:
    ack: Future[str] = Future()

    @template.listener(test_queue, on_error=ack.set_exception)
    def server(message: str, ctx: MessageContext) -> None:  # type: ignore
        reply_queue = ctx.reply_destination
        if reply_queue is None:
            raise RuntimeError("reply queue is None")
        template.send(reply_queue, "ack", correlation_id=ctx.correlation_id)
        ack.set_result("ack")

    reply_future = template.send_and_receive(test_queue, "test", str)
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
    template: MessagingTemplate,
    test_queue: MessageQueue,
    message: Any,
    message_type: type,
) -> None:
    ack: Future[message_type] = Future()  # type: ignore

    def server(message: message_type, ctx: MessageContext) -> None:  # type: ignore
        reply_queue = ctx.reply_destination
        if reply_queue is None:
            raise RuntimeError("reply queue is None")
        template.send(reply_queue, message, correlation_id=ctx.correlation_id)
        ack.set_result(message)  # type: ignore

    template.subscribe(test_queue, server, on_error=ack.set_exception)  # type: ignore

    reply_future: Future[message_type] = template.send_and_receive(  # type: ignore
        test_queue, message, message_type
    )
    result = ack.result(timeout=_TIMEOUT)  # type: ignore
    if type(message) is np.ndarray:
        message = message.tolist()
    assert result == message
    assert reply_future.result(timeout=_TIMEOUT) == message


def test_subscribe_before_connect(
    disconnected_template: MessagingTemplate, test_queue: MessageQueue
) -> None:
    acknowledge(disconnected_template, test_queue)
    disconnected_template.connect()
    reply = disconnected_template.send_and_receive(test_queue, "test", str).result(
        timeout=_TIMEOUT
    )
    assert reply == "ack"


def test_reconnect(template: MessagingTemplate, test_queue: MessageQueue) -> None:
    acknowledge(template, test_queue)
    reply = template.send_and_receive(test_queue, "test", str).result(timeout=_TIMEOUT)
    assert reply == "ack"
    template.disconnect()
    assert not template.is_connected()
    template.connect()
    assert template.is_connected()
    reply = template.send_and_receive(test_queue, "test", str).result(timeout=_TIMEOUT)
    assert reply == "ack"


def test_correlation_id(
    template: MessagingTemplate, test_queue: MessageQueue, test_queue_2: MessageQueue
) -> None:
    correlation_id = "foobar"
    q: Queue[MessageContext] = Queue()

    def server(msg: str, ctx: MessageContext) -> None:
        q.put(ctx)
        template.send(test_queue_2, msg, correlation_id=ctx.correlation_id)

    def client(msg: str, ctx: MessageContext) -> None:
        q.put(ctx)

    template.subscribe(test_queue, server)
    template.subscribe(test_queue_2, client)
    template.send(test_queue, "test", correlation_id=correlation_id)

    ctx_req: MessageContext = q.get(timeout=_TIMEOUT)
    assert ctx_req.correlation_id == correlation_id
    ctx_ack: MessageContext = q.get(timeout=_TIMEOUT)
    assert ctx_ack.correlation_id == correlation_id


def acknowledge(template: MessagingTemplate, destination: DestinationBase) -> None:
    def server(message: str, ctx: MessageContext) -> None:
        reply_queue = ctx.reply_destination
        if reply_queue is None:
            raise RuntimeError("reply queue is None")
        template.send(reply_queue, "ack", correlation_id=ctx.correlation_id)

    template.subscribe(destination, server)
