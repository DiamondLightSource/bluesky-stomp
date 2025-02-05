from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np
import pytest
from pydantic import BaseModel

from bluesky_stomp.messaging import (
    deserialize_message,
    determine_callback_deserialization_type,
    serialize_message,
)


class Foo(BaseModel):
    bar: int
    baz: str


class Bar(BaseModel):
    unique: set[int]
    repeated: list[int]


def test_determine_callback_deserialization_type() -> None:
    def on_message(message: Foo, headers: Mapping[str, Any]) -> None: ...

    deserialization_type = determine_callback_deserialization_type(on_message)
    assert deserialization_type is Foo


def test_determine_callback_deserialization_type_with_another_parameter() -> None:
    def on_message(headers: Mapping[str, Any], message: Foo) -> None: ...

    deserialization_type = determine_callback_deserialization_type(
        on_message, parameter_index=1
    )
    assert deserialization_type is Foo


@pytest.mark.parametrize("parameter_index", [2, 3])
def test_determine_callback_deserialization_type_with_out_of_bounds_parameter(
    parameter_index: int,
) -> None:
    def on_message(message: Foo, headers: Mapping[str, Any]) -> None: ...

    with pytest.raises(
        IndexError, match="Cannot find a deserialization type for parameter"
    ):
        determine_callback_deserialization_type(
            on_message,
            parameter_index=parameter_index,
        )


def test_determine_callback_deserialization_type_with_no_type_defaults_to_str() -> None:
    def on_message(message, headers: Mapping[str, Any]) -> None: ...  # type: ignore

    deserialization_type = determine_callback_deserialization_type(on_message)  # type: ignore
    assert deserialization_type is str


def test_determine_callback_deserialization_type_with_default() -> None:
    def on_message(message, headers: Mapping[str, Any]) -> None: ...  # type: ignore

    deserialization_type = determine_callback_deserialization_type(
        on_message,  # type: ignore
        default=int,
    )
    assert deserialization_type is int


def test_determine_callback_deserialization_type_with_empty_signature() -> None:
    def on_message() -> None: ...

    with pytest.raises(
        IndexError, match="Cannot find a deserialization type for parameter"
    ):
        determine_callback_deserialization_type(on_message)


@dataclass
class DeAndSerializationTestCase:
    obj: Any
    as_bytes: bytes
    obj_type: type[Any]


DE_SERIALIZATION_TEST_CASES = [
    DeAndSerializationTestCase("test", b'"test"', str),
    DeAndSerializationTestCase(1, b"1", int),
    DeAndSerializationTestCase(False, b"false", bool),
    DeAndSerializationTestCase([1, 2, 3], b"[1,2,3]", list),
    DeAndSerializationTestCase({1, 2, 3}, b"[1,2,3]", set),
    DeAndSerializationTestCase({"foo": 1}, b'{"foo":1}', dict),
    DeAndSerializationTestCase(Foo(bar=1, baz="baz"), b'{"bar":1,"baz":"baz"}', Foo),
    DeAndSerializationTestCase(np.array([1, 2, 3]), b"[1,2,3]", list),
    DeAndSerializationTestCase(
        Bar(unique={1, 2, 3}, repeated=[1, 1, 2, 2, 3, 3]),
        b'{"unique":[1,2,3],"repeated":[1,1,2,2,3,3]}',
        Bar,
    ),
]


@pytest.mark.parametrize("test_case", DE_SERIALIZATION_TEST_CASES)
def test_serialization(test_case: DeAndSerializationTestCase) -> None:
    assert serialize_message(test_case.obj) == test_case.as_bytes


@pytest.mark.parametrize("test_case", DE_SERIALIZATION_TEST_CASES)
def test_deserialization(test_case: DeAndSerializationTestCase) -> None:
    assert np.array_equal(
        deserialize_message(test_case.as_bytes, test_case.obj_type), test_case.obj
    )


def test_unserializable():
    class NewType:
        pass

    with pytest.raises(TypeError):
        serialize_message(NewType())
