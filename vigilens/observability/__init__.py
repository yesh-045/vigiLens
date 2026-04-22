"""
Observability module for VigiLens.
Provides configuration and decorators for Opik integration.
"""

from .config import get_opik_config, configure_opik
from .decorators import trace, track

__all__ = ["get_opik_config", "configure_opik", "trace", "track"]
