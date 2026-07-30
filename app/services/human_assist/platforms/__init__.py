"""Human Assist platform registry and built-in adapters."""

from app.services.human_assist.platforms.base import (
    ConversationEvent,
    ConversationSource,
    EndConversationEvent,
    EndConversationInitiator,
    HandoffEvent,
    HumanAssistPlatform,
    HumanAssistPlatformContext,
    HumanAssistPlatformError,
    HumanAssistPlatformSpec,
    PlatformMessage,
    PlatformOperationResult,
)
from app.services.human_assist.platforms.native import NativeHumanAssistPlatform
from app.services.human_assist.platforms.registry import (
    get_platform,
    is_platform_registered,
    list_platforms,
    register_platform,
)

register_platform(NativeHumanAssistPlatform())

__all__ = [
    "ConversationEvent",
    "ConversationSource",
    "EndConversationEvent",
    "EndConversationInitiator",
    "HandoffEvent",
    "HumanAssistPlatform",
    "HumanAssistPlatformContext",
    "HumanAssistPlatformError",
    "HumanAssistPlatformSpec",
    "PlatformMessage",
    "PlatformOperationResult",
    "get_platform",
    "is_platform_registered",
    "list_platforms",
    "register_platform",
]
