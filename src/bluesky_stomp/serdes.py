import inspect
from collections.abc import Callable
from typing import Any, TypeVar, cast

import orjson
from pydantic import BaseModel, TypeAdapter

T = TypeVar("T")


MessageSerializer = Callable[[Any], bytes]
MessageDeserializer = Callable[[str | bytes | bytearray, type[T]], T]


def serialize_message(message: Any) -> bytes:
    """
    Pydantic-aware serialization routine that can also be
    used on primitives. So serialize(4) is "4", but
    serialize(<model>) is a JSON string.

    Args:
        obj: The object to serialize

    Returns:
        str: The serialized object
    """

    if isinstance(message, BaseModel):
        # Serialize by alias so that our camelCase models leave the service
        # with camelCase field names
        json_serializable = message.model_dump(by_alias=True)
    else:
        json_serializable = message

    return orjson.dumps(
        json_serializable,
        option=orjson.OPT_SERIALIZE_NUMPY,
        default=_fallback_serialize,
    )


def deserialize_message(message: str | bytes | bytearray, obj_type: type[T]) -> T:
    """
    Deserialize a message into a useful type

    Args:
        message: Message received from stomp
        obj_type: Target type for deserialization

    Returns:
        T: An instance of obj_type
    """

    as_primitive = orjson.loads(message)
    adapter: TypeAdapter[Any] = TypeAdapter(obj_type)
    return adapter.validate_python(as_primitive)


def determine_callback_deserialization_type(
    listener: Callable[..., Any],
    default: type = str,
    parameter_index: int = 0,
) -> type[Any]:
    """
    Inspect a function that is intended for listening
    to messages and determine the type for deserialization based on the
    type hints in the function signature.

    Args:
        listener: A function that takes at least one parameter, the deserialized message
        default: A default type to fall back on if no type is supplied.
        Defaults to str.
        parameter_index: The index of the message parameter in the function signature.
        Defaults to 0.

    Raises:
        IndexError: If parameter_index is out of bounds for the number of parameters the
        function actually takes

    Returns:
        type[Any]: The type for deserialization
    """

    parameters = inspect.signature(listener).parameters.values()
    if parameter_index >= len(parameters):
        raise IndexError(
            "Cannot find a deserialization type for "
            f"parameter {parameter_index} of {listener} (0-indexed), ",
            f"it only takes {len(parameters)} parameters.",
        )
    message = list(parameters)[parameter_index]
    a_type = message.annotation
    if a_type is not inspect.Parameter.empty:
        return a_type
    else:
        return default


def _fallback_serialize(obj: Any) -> Any:
    if isinstance(obj, set):
        return list(cast(set[Any], obj))
    raise TypeError(f"Type {type(obj)} not serializable")
