import re
from concurrent.futures import Future
from unittest.mock import ANY, Mock

import pytest
from stomp.connect import (  # type: ignore
    Frame,
)
from stomp.connect import StompConnection11 as Connection  # type: ignore

from bluesky_stomp.messaging import (
    CORRELATION_ID_HEADER,
    MessageContext,
    MessagingTemplate,
)
from bluesky_stomp.models import (
    Broker,
    DestinationBase,
    MessageQueue,
    MessageTopic,
    ReplyMessageQueue,
    TemporaryMessageQueue,
)


@pytest.fixture
def mock_connection() -> Mock:
    return Mock(spec=Connection)


@pytest.fixture
def template(mock_connection: Mock) -> MessagingTemplate:
    return MessagingTemplate(conn=mock_connection)


@pytest.fixture
def mock_listener(mock_connection: Mock, template: MessagingTemplate) -> Mock:
    return mock_connection.set_listener.mock_calls[0].args[1]


def test_for_broker_constructor():
    MessagingTemplate.for_broker(Broker.localhost())


@pytest.mark.parametrize(
    "destination,expected_raw_destination",
    [
        (MessageQueue(name="foo"), "/queue/foo"),
        (TemporaryMessageQueue(name="foo"), "/temp-queue/foo"),
        (ReplyMessageQueue(name="foo"), "/reply-queue/foo"),
        (MessageTopic(name="foo"), "/topic/foo"),
    ],
)
def test_sends_to_correct_destination(
    mock_connection: Mock,
    template: MessagingTemplate,
    destination: DestinationBase,
    expected_raw_destination: str,
):
    template.send(destination, "misc")
    mock_connection.send.assert_called_once_with(
        headers=ANY,
        body=ANY,
        destination=expected_raw_destination,
    )


def test_sends_serialized_message(mock_connection: Mock, template: MessagingTemplate):
    template.send(MessageQueue(name="misc"), {"foo": 1})
    mock_connection.send.assert_called_once_with(
        headers=ANY,
        body='{"foo": 1}',
        destination=ANY,
    )


def test_sends_jms_headers(mock_connection: Mock, template: MessagingTemplate):
    template.send(MessageQueue(name="misc"), "misc")
    mock_connection.send.assert_called_once_with(
        headers={"JMSType": "TextMessage"},
        body=ANY,
        destination=ANY,
    )


def test_sends_correlation_id(mock_connection: Mock, template: MessagingTemplate):
    template.send(MessageQueue(name="misc"), "misc", correlation_id="foo")
    mock_connection.send.assert_called_once_with(
        headers={
            "JMSType": "TextMessage",
            CORRELATION_ID_HEADER: "foo",
        },
        body=ANY,
        destination=ANY,
    )


def test_sends_reply_queue(mock_connection: Mock, template: MessagingTemplate):
    template.send(
        MessageQueue(name="misc"),
        "misc",
        on_reply=lambda msg, context: None,
    )
    mock_connection.send.assert_called_once_with(
        headers={
            "JMSType": "TextMessage",
            "reply-to": ANY,
        },
        body=ANY,
        destination=ANY,
    )


def test_subscribes_to_replies(mock_connection: Mock, template: MessagingTemplate):
    template.send(
        MessageQueue(name="misc"),
        "misc",
        on_reply=lambda msg, context: None,
    )
    mock_connection.subscribe.assert_called_once()


def test_send_and_receive_calls_subscribe(
    mock_connection: Mock, template: MessagingTemplate
):
    mock_connection.is_connected.return_value = True
    template.send_and_receive(MessageQueue(name="misc"), "message")
    mock_connection.subscribe.assert_called_once()
    kwargs = mock_connection.subscribe.mock_calls[0].kwargs
    destination = kwargs["destination"]
    sub_id = kwargs["id"]
    for queue in [destination, sub_id]:
        assert len(re.compile(r"/temp-queue/.*").findall(queue)) == 1


def test_subscribe_before_connect(mock_connection: Mock, template: MessagingTemplate):
    mock_connection.is_connected.return_value = False

    template.subscribe(MessageQueue(name="misc"), lambda msg, context: None)
    mock_connection.subscribe.assert_not_called()

    mock_connection.is_connected.return_value = True
    template.connect()

    mock_connection.subscribe.assert_called_once_with(
        destination=ANY,
        id="0",
        ack="auto",
    )


def test_subscribe_after_connect(mock_connection: Mock, template: MessagingTemplate):
    mock_connection.is_connected.return_value = True

    template.subscribe(MessageQueue(name="misc"), lambda msg, context: None)

    mock_connection.subscribe.assert_called_once_with(
        destination=ANY,
        id="0",
        ack="auto",
    )


def test_connect_is_idempotent(mock_connection: Mock, template: MessagingTemplate):
    mock_connection.is_connected.return_value = True
    template.connect()
    mock_connection.connect.assert_not_called()


def test_disconnect_is_idempotent(mock_connection: Mock, template: MessagingTemplate):
    mock_connection.is_connected.return_value = False
    template.disconnect()
    mock_connection.disconnect.assert_not_called()


def test_listener_subscribes(mock_connection: Mock, template: MessagingTemplate):
    mock_connection.is_connected.return_value = True

    @template.listener(destination=MessageQueue(name="misc"))
    def callback(message: str, context: MessageContext) -> None: ...  # type: ignore

    mock_connection.subscribe.assert_called_once_with(
        destination="/queue/misc",
        id="0",
        ack="auto",
    )


def test_callback_propagates(
    mock_listener: Mock,
    mock_connection: Mock,
    template: MessagingTemplate,
):
    mock_connection.is_connected.return_value = True

    future: Future[str] = Future()

    def callback(message: str, context: MessageContext) -> None:
        future.set_result(message)

    template.subscribe(
        MessageQueue(name="misc"), callback, on_error=future.set_exception
    )

    mock_listener.on_message(
        Frame(
            cmd="RECV",
            headers={
                "subscription": "0",
                "destination": "/queue/misc",
            },
            body='"foo"',
        )
    )
    assert future.result(timeout=1.0) == "foo"


def test_send_and_receive_propagates(
    mock_listener: Mock,
    mock_connection: Mock,
    template: MessagingTemplate,
):
    mock_connection.is_connected.return_value = True

    future = template.send_and_receive(MessageQueue(name="misc"), "message")
    temporary_queue_name = mock_connection.subscribe.mock_calls[0].kwargs["destination"]

    mock_listener.on_message(
        Frame(
            cmd="RECV",
            headers={
                "subscription": temporary_queue_name,
                "destination": "/queue/misc",
            },
            body='"foo"',
        )
    )
    assert future.result(timeout=1.0) == "foo"


def test_subscription_error_handling(
    mock_listener: Mock,
    mock_connection: Mock,
    template: MessagingTemplate,
):
    mock_connection.is_connected.return_value = True

    future: Future[str] = Future()

    def callback(message: str, context: MessageContext) -> None:
        raise RuntimeError()

    template.subscribe(
        MessageQueue(name="misc"), callback, on_error=future.set_exception
    )

    with pytest.raises(RuntimeError):
        mock_listener.on_message(
            Frame(
                cmd="RECV",
                headers={
                    "subscription": "0",
                    "destination": "/queue/misc",
                },
                body='"foo"',
            )
        )
        future.result(timeout=0)  # Should raise an exception
