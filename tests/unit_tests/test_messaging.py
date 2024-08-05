from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pytest

from bluesky_stomp.messaging import determine_deserialization_type


@dataclass
class Foo:
    bar: int
    baz: str


def test_determine_deserialization_type() -> None:
    def on_message(message: Foo, headers: Mapping[str, Any]) -> None: ...

    deserialization_type = determine_deserialization_type(on_message)  # type: ignore
    assert deserialization_type is Foo


def test_determine_deserialization_type_with_no_type() -> None:
    def on_message(message, headers: Mapping[str, Any]) -> None: ...  # type: ignore

    deserialization_type = determine_deserialization_type(on_message)  # type: ignore
    assert deserialization_type is str


def test_determine_deserialization_type_with_wrong_signature() -> None:
    def on_message(message: Foo) -> None: ...

    with pytest.raises(ValueError):
        determine_deserialization_type(on_message)  # type: ignore
