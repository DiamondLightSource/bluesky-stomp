from typing import cast

import pytest
from observability_utils.tracing import (  # type: ignore
    JsonObjectSpanExporter,
    set_console_exporter,
    setup_tracing,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import get_tracer_provider


@pytest.fixture(scope="session", autouse=True)
def setup():
    setup_tracing("test", False)
    set_console_exporter()


@pytest.fixture(scope="session")
def exporter() -> JsonObjectSpanExporter:
    exporter = JsonObjectSpanExporter()
    provider = cast(TracerProvider, get_tracer_provider())
    # Use SimpleSpanProcessor to keep tests quick
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter
