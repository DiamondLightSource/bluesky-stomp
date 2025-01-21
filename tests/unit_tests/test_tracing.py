from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
from opentelemetry.propagate import get_global_textmap
from opentelemetry.trace.span import NonRecordingSpan
from stomp.connect import StompConnection11 as Connection  # type: ignore

from bluesky_stomp.messaging import StompClient
from bluesky_stomp.models import MessageQueue


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
def mock_get_tracer():
    """Patches messaging.get_tracer with a mock that returns a MagicMock when called"""
    with patch("bluesky_stomp.messaging.get_tracer") as get_tracer:
        mock_tracer = MagicMock()

        def side_effect():
            return mock_tracer

        get_tracer.side_effect = side_effect()
        yield get_tracer


def test_send_and_receive_starts_span():
    raise NotImplementedError


def test_sends_tracer_headers(
    mock_connection: Mock,
    client: StompClient,
):
    client.send(MessageQueue(name="misc"), "misc")
    mock_connection.send.assert_called_once_with(
        headers={
            "JMSType": "TextMessage",
            "traceparent": ANY,
        },
        body=ANY,
        destination=ANY,
    )


def test_send_starts_span(
    mock_connection: Mock, client: StompClient, mock_get_tracer: Mock
):
    client.send(MessageQueue(name="misc"), "misc")

    mock_tracer = mock_get_tracer()
    mock_tracer.start_as_current_span.assert_called()


def test_long_process_starts_different_traces(
    mock_connection: Mock,
    client: StompClient,
):
    for _ in range(2):
        client.send(MessageQueue(name="misc"), "misc")

    carrier_1 = mock_connection.send.call_args_list[0][1]["headers"]
    carrier_2 = mock_connection.send.call_args_list[1][1]["headers"]

    context_1 = get_global_textmap().extract(carrier=carrier_1)
    context_2 = get_global_textmap().extract(carrier=carrier_2)

    # I hope this is robust
    non_recording_span_1: NonRecordingSpan = list(context_1.values())[0]  # type: ignore
    non_recording_span_2: NonRecordingSpan = list(context_2.values())[0]  # type: ignore

    trace_id_1: int = non_recording_span_1.get_span_context().trace_id
    trace_id_2: int = non_recording_span_2.get_span_context().trace_id

    assert trace_id_1 != trace_id_2


def test_send_bytes_starts_span():
    raise NotImplementedError


def test_subscribe_starts_span():
    raise NotImplementedError


def test_connect_starts_span():
    raise NotImplementedError


def test_connect_span_has_success_attribute():
    raise NotImplementedError


def test_disconnect_starts_span():
    raise NotImplementedError


def test_disconnect_span_has_success_attribute():
    raise NotImplementedError


def test_on_message_starts_span():
    raise NotImplementedError
