"""
CRM-related integrations module.

Currently contains Wahelp API client utilities.
"""

from .wahelp_client import (
    WahelpAPIError,
    WahelpClient,
    WahelpProjectConfig,
)
from .wahelp_service import (
    WahelpChannelConfig,
    get_channel_uuid,
    get_project_config,
    get_wahelp_client,
)

__all__ = [
    "WahelpAPIError",
    "WahelpClient",
    "WahelpProjectConfig",
    "WahelpChannelConfig",
    "get_channel_uuid",
    "get_project_config",
    "get_wahelp_client",
]
