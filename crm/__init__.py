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
    ChannelKind,
    ensure_user_in_channel,
    get_channel_uuid,
    get_project_config,
    get_wahelp_client,
    send_text_message,
    send_text_to_phone,
)
from .wahelp_dispatcher import (
    ClientContact,
    SendResult,
    cancel_followup_for_client,
    schedule_followup_for_client,
    set_missing_messenger_logger,
    send_with_rules,
)

__all__ = [
    "WahelpAPIError",
    "WahelpClient",
    "WahelpProjectConfig",
    "WahelpChannelConfig",
    "ChannelKind",
    "get_channel_uuid",
    "get_project_config",
    "get_wahelp_client",
    "ensure_user_in_channel",
    "send_text_message",
    "send_text_to_phone",
    "ClientContact",
    "SendResult",
    "send_with_rules",
    "cancel_followup_for_client",
    "schedule_followup_for_client",
    "set_missing_messenger_logger",
]
