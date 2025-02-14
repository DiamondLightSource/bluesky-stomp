import re
from concurrent.futures import Future
from unittest.mock import ANY, Mock, patch

import pytest
from stomp.connect import ConnectFailedException, Frame  # type: ignore
from stomp.connect import StompConnection11 as Connection  # type: ignore

from bluesky_stomp.messaging import CORRELATION_ID_HEADER, MessageContext, StompClient
from bluesky_stomp.models import (
    BasicAuthentication,
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
def client(mock_connection: Mock) -> StompClient:
    return StompClient(conn=mock_connection)


# Depends on client to ensure fixtures are executed in correct order
@pytest.fixture
def mock_listener(mock_connection: Mock, client: StompClient) -> Mock:
    return mock_connection.set_listener.mock_calls[0].args[1]


@pytest.fixture()
def failing_client(mock_connection: Mock) -> StompClient:
    mock_connection.connect.side_effect = ConnectFailedException
    return StompClient(mock_connection)


def test_for_broker_constructor():
    StompClient.for_broker(Broker.localhost())


def test_failed_connect(
    mock_connection: Mock,
    failing_client: StompClient,
) -> None:
    mock_connection.is_connected.return_value = False
    assert not failing_client.is_connected()
    with pytest.raises(ConnectFailedException):
        failing_client.connect()
    assert not failing_client.is_connected()


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
    client: StompClient,
    destination: DestinationBase,
    expected_raw_destination: str,
):
    client.send(destination, "misc")
    mock_connection.send.assert_called_once_with(
        headers=ANY,
        body=ANY,
        destination=expected_raw_destination,
    )


def test_sends_serialized_message(mock_connection: Mock, client: StompClient):
    client.send(MessageQueue(name="misc"), {"foo": 1})
    mock_connection.send.assert_called_once_with(
        headers=ANY,
        body=b'{"foo":1}',
        destination=ANY,
    )


def test_sends_jms_headers(mock_connection: Mock, client: StompClient):
    client.send(MessageQueue(name="misc"), "misc")
    mock_connection.send.assert_called_once_with(
        headers={
            "JMSType": "TextMessage",
            "traceparent": ANY,
        },
        body=ANY,
        destination=ANY,
    )


def test_sends_correlation_id(mock_connection: Mock, client: StompClient):
    client.send(MessageQueue(name="misc"), "misc", correlation_id="foo")
    mock_connection.send.assert_called_once_with(
        headers={
            "JMSType": "TextMessage",
            CORRELATION_ID_HEADER: "foo",
            "traceparent": ANY,
        },
        body=ANY,
        destination=ANY,
    )


def test_sends_reply_queue(mock_connection: Mock, client: StompClient):
    client.send(
        MessageQueue(name="misc"),
        "misc",
        on_reply=lambda msg, context: None,
    )
    mock_connection.send.assert_called_once_with(
        headers={
            "JMSType": "TextMessage",
            "reply-to": ANY,
            "traceparent": ANY,
        },
        body=ANY,
        destination=ANY,
    )


def test_subscribes_to_replies(mock_connection: Mock, client: StompClient):
    client.send(
        MessageQueue(name="misc"),
        "misc",
        on_reply=lambda msg, context: None,
    )
    mock_connection.subscribe.assert_called_once()


def test_send_and_receive_calls_subscribe(mock_connection: Mock, client: StompClient):
    mock_connection.is_connected.return_value = True
    client.send_and_receive(MessageQueue(name="misc"), "message")
    mock_connection.subscribe.assert_called_once()
    kwargs = mock_connection.subscribe.mock_calls[0].kwargs
    destination = kwargs["destination"]
    sub_id = kwargs["id"]
    for queue in [destination, sub_id]:
        assert len(re.compile(r"/temp-queue/.*").findall(queue)) == 1


def test_subscribe_before_connect(mock_connection: Mock, client: StompClient):
    mock_connection.is_connected.return_value = False

    client.subscribe(MessageQueue(name="misc"), lambda msg, context: None)
    mock_connection.subscribe.assert_not_called()

    mock_connection.is_connected.return_value = True
    client.connect()

    mock_connection.subscribe.assert_called_once_with(
        destination=ANY,
        id="0",
        ack="auto",
    )


def test_subscribe_after_connect(mock_connection: Mock, client: StompClient):
    mock_connection.is_connected.return_value = True

    client.subscribe(MessageQueue(name="misc"), lambda msg, context: None)

    mock_connection.subscribe.assert_called_once_with(
        destination=ANY,
        id="0",
        ack="auto",
    )


def test_connect_is_idempotent(mock_connection: Mock, client: StompClient):
    mock_connection.is_connected.return_value = True
    client.connect()
    mock_connection.connect.assert_not_called()


def test_disconnect_is_idempotent(mock_connection: Mock, client: StompClient):
    mock_connection.is_connected.return_value = False
    client.disconnect()
    mock_connection.disconnect.assert_not_called()


def test_listener_subscribes(mock_connection: Mock, client: StompClient):
    mock_connection.is_connected.return_value = True

    @client.listener(destination=MessageQueue(name="misc"))
    def callback(message: str, context: MessageContext) -> None: ...  # type: ignore

    mock_connection.subscribe.assert_called_once_with(
        destination="/queue/misc",
        id="0",
        ack="auto",
    )


def test_callback_propagates(
    mock_listener: Mock,
    mock_connection: Mock,
    client: StompClient,
):
    mock_connection.is_connected.return_value = True

    future: Future[str] = Future()

    def callback(message: str, context: MessageContext) -> None:
        future.set_result(message)

    client.subscribe(MessageQueue(name="misc"), callback, on_error=future.set_exception)

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
    client: StompClient,
):
    mock_connection.is_connected.return_value = True

    future = client.send_and_receive(MessageQueue(name="misc"), "message")
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
    client: StompClient,
):
    mock_connection.is_connected.return_value = True

    future: Future[str] = Future()

    def callback(message: str, context: MessageContext) -> None:
        raise RuntimeError()

    client.subscribe(MessageQueue(name="misc"), callback, on_error=future.set_exception)

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


def test_subscription_default_error_handling(
    mock_listener: Mock,
    mock_connection: Mock,
    client: StompClient,
):
    mock_connection.is_connected.return_value = True

    def callback(message: str, context: MessageContext) -> None:
        raise RuntimeError("test_subscription_default_error_handling")

    client.subscribe(MessageQueue(name="misc"), callback)

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


def test_connect_error_propagated(
    mock_connection: Mock,
    client: StompClient,
):
    mock_connection.is_connected.return_value = False
    mock_connection.connect.side_effect = ConnectFailedException

    with pytest.raises(ConnectFailedException):
        client.connect()


@patch("bluesky_stomp.messaging.Event")
def test_connect_connects_to_bus(
    mock_event: Mock,
    mock_connection: Mock,
    client: StompClient,
):
    mock_connection.is_connected.return_value = False
    client.connect()
    mock_connection.connect.assert_called_once_with(wait=True)


@patch("bluesky_stomp.messaging.Event")
def test_connect_synchs_on_listener(
    mock_event: Mock,
    mock_listener: Mock,
    mock_connection: Mock,
    client: StompClient,
):
    mock_event_instance = Mock()
    mock_event.return_value = mock_event_instance

    mock_connection.is_connected.return_value = False
    client.connect()

    mock_event_instance.wait.assert_called_once()
    mock_event_instance.set.assert_not_called()

    mock_listener.on_connected(...)

    mock_event_instance.set.assert_called_once()


@patch("bluesky_stomp.messaging.Event")
@patch("bluesky_stomp.messaging.time.sleep")
def test_disconnect_polls(
    mock_event: Mock,
    mock_sleep: Mock,
    mock_listener: Mock,
    mock_connection: Mock,
    client: StompClient,
):
    mock_connection.is_connected.side_effect = [
        False,
        False,
        True,
        True,
        False,
        False,
        False,
        True,
    ]
    mock_connection.connect.side_effect = [
        None,
        ConnectFailedException,
        ConnectFailedException,
        None,
    ]
    client.connect()

    assert mock_connection.connect.call_count == 1

    mock_listener.on_disconnected()
    assert mock_connection.connect.call_count == 2


@patch("bluesky_stomp.messaging.Event")
def test_connect_passes_basic_auth_details(
    mock_event: Mock,
    mock_connection: Mock,
):
    authentication = BasicAuthentication(
        username="foo",
        password="bar",  # type: ignore  https://github.com/pydantic/pydantic/issues/9557
    )
    client = StompClient(
        conn=mock_connection,
        authentication=authentication,
    )
    mock_connection.is_connected.return_value = False
    client.connect()
    mock_connection.connect.assert_called_once_with(
        username=authentication.username,
        passcode=authentication.password.get_secret_value(),
        wait=True,
    )


@patch("bluesky_stomp.messaging.Event")
def test_disconnect_disconnects_from_bus(
    mock_event: Mock,
    mock_connection: Mock,
    client: StompClient,
):
    mock_connection.is_connected.return_value = True
    client.disconnect()
    mock_connection.disconnect.assert_called_once()
