import os
import logging
from typing import Optional
from pydantic import BaseModel, Field
import opik

logger = logging.getLogger(__name__)


class OpikConfig(BaseModel):
    """Opik configuration settings."""

    project_name: str = Field(default="vigilens", description="The Opik project name")
    workspace: Optional[str] = Field(default=None, description="The Opik workspace")
    api_key: Optional[str] = Field(default=None, description="The Opik API key")
    host: Optional[str] = Field(
        default="https://www.comet.com/opik/api", description="The Opik host URL"
    )
    enabled: bool = Field(default=True, description="Whether Opik tracing is enabled")


def get_opik_config() -> OpikConfig:
    """Retrieve Opik configuration from environment variables."""
    # We prefix env vars with OPIK_ or VIGILENS_OPIK_
    enabled_str = os.getenv("VIGILENS_OPIK_ENABLED", "true").lower()
    return OpikConfig(
        project_name=os.getenv("OPIK_PROJECT_NAME", "vigilens"),
        workspace=os.getenv("OPIK_WORKSPACE"),
        api_key=os.getenv("OPIK_API_KEY"),
        host=os.getenv("OPIK_URL_OVERRIDE", "https://www.comet.com/opik/api"),
        enabled=enabled_str in ("true", "1", "yes"),
    )


def configure_opik() -> None:
    """Configure the Opik client if enabled."""
    config = get_opik_config()

    if not config.enabled:
        logger.info("Opik observability is disabled.")
        return

    # Set environment variables for the opik client
    if config.api_key:
        os.environ["OPIK_API_KEY"] = config.api_key
    if config.workspace:
        os.environ["OPIK_WORKSPACE"] = config.workspace
    if config.project_name:
        os.environ["OPIK_PROJECT_NAME"] = config.project_name
    if config.host:
        os.environ["OPIK_URL_OVERRIDE"] = config.host

    try:
        # Initialize Opik - this sets up the global client
        opik.configure(use_local=False)
        logger.info(f"Opik configured successfully for project: {config.project_name}")
    except Exception as e:
        logger.error(f"Failed to configure Opik: {e}")
