import functools
import logging
from typing import Any, Callable, TypeVar, cast
import opik

from .config import get_opik_config

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def trace( name: str | None = None, capture_inputs: bool = True, capture_outputs: bool = True) -> Callable[[F], F]:
    """
    Decorator for tracing a function with Opik.

    Args:
        name: Override the trace name (defaults to function name).
        capture_inputs: Whether to trace function inputs.
        capture_outputs: Whether to trace function outputs.
    """
    config = get_opik_config()

    def decorator(func: F) -> F:
        if not config.enabled:
            return func

        try:
            # Apply the opik.track decorator
            opik_decorator = opik.track(
                name=name or func.__name__,
                capture_inputs=capture_inputs,
                capture_outputs=capture_outputs,
            )
            return opik_decorator(func)  # type: ignore
        except Exception as e:
            logger.warning(f"Could not apply opik.track to {func.__name__}: {e}")
            return func

    return decorator


# Alias for backward compatibility / explicit choice
track = trace
