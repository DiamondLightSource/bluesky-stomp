from unittest.mock import ANY, Mock, patch

import pytest
from observability_utils.tracing import get_tracer, setup_tracing  # type: ignore
from opentelemetry.trace import Tracer
from stomp.connect import StompConnection11 as Connection  # type: ignore

from bluesky_stomp.messaging import StompClient
from bluesky_stomp.models import MessageQueue

setup_tracing("test_tracing", False)


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


@pytest.fixture
def mock_tracer():
    with patch("observability_utils.tracing.helpers.Tracer") as tracer:
        yield tracer


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


def test_starts_span(mock_connection: Mock, client: StompClient, mock_tracer: Mock):
    client.send(MessageQueue(name="misc"), "misc")
    mock_tracer.start_as_current_span.assert_called()
