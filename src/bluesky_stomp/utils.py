import sys
import traceback
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


def print_exception_to_stderr(e: Exception) -> None:
    print(f"Exception in thread: {e}", file=sys.stderr)
    print(traceback.format_exc(), file=sys.stderr)


def handle_all_exceptions(
    func: Callable[P, T],
    callback: Callable[[Exception], None] = print_exception_to_stderr,
) -> Callable[P, T]:
    """
    Ensure any uncaught exception traceback is printed to stdout. This does not
    happen by default in threads other than the main thread. This function can
    also be used as a decorator.

    Args:
        func (Callable[..., Any]): The function to wrap
        callback (Optional[Callable[[Exception], None]], optional): Error handling
                                                                    function, defaults
                                                                    to printing a stack
                                                                    trace to stderr.

    Returns:
        Callable: Wrapped function that prints exception traceback
    """

    callback = callback or print_exception_to_stderr

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            callback(e)
            raise e

    return wrapper
