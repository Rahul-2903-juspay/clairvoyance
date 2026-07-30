"""Three-operation contract for every Human Assist platform integration."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class ConversationSource(str, Enum):
    """The side that produced a conversation event."""

    CUSTOMER = "customer"
    MERCHANT = "merchant"
    PLATFORM = "platform"


class EndConversationInitiator(str, Enum):
    """The side or lifecycle rule that ended a handoff."""

    CUSTOMER = "customer"
    MERCHANT = "merchant"
    PLATFORM = "platform"
    TIMEOUT = "timeout"
    SESSION = "session"
    SYSTEM = "system"


@dataclass(frozen=True, slots=True)
class HumanAssistPlatformSpec:
    """Static registry metadata; adding a platform does not change schemas."""

    key: str
    display_name: str
    description: str


@dataclass(frozen=True, slots=True)
class HumanAssistPlatformContext:
    """Tenant and conversation identity supplied by the orchestrator."""

    conversation_id: str
    chat_session_id: str
    widget_config_id: str
    reseller_id: str
    platform: str
    merchant_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HandoffEvent:
    """Buddy/customer history supplied when opening a new handoff."""

    transcript: Tuple[Dict[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class ConversationEvent:
    """One bidirectional message/event handled by the selected platform."""

    source: ConversationSource
    content: Optional[str] = None
    actor_id: Optional[str] = None
    raw_payload: Optional[Dict[str, Any]] = None


@dataclass(frozen=True, slots=True)
class EndConversationEvent:
    """A terminal event from either side or from a lifecycle rule."""

    initiator: EndConversationInitiator
    reason: str
    actor_id: Optional[str] = None
    raw_payload: Optional[Dict[str, Any]] = None


@dataclass(frozen=True, slots=True)
class PlatformMessage:
    """Provider-normalized message that the orchestrator persists canonically."""

    content: str
    sender_type: str
    actor_id: Optional[str] = None
    external_id: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True, slots=True)
class PlatformOperationResult:
    """Provider result folded into the conversation's existing metadata JSONB."""

    conversation_ref: Optional[str] = None
    provider_state: Dict[str, Any] = field(default_factory=dict)
    messages: Tuple[PlatformMessage, ...] = ()


class HumanAssistPlatform(ABC):
    """A platform integration implements exactly these three operations."""

    spec: HumanAssistPlatformSpec

    @abstractmethod
    async def handoff(
        self,
        context: HumanAssistPlatformContext,
        event: HandoffEvent,
    ) -> PlatformOperationResult:
        """Create or open the platform conversation for the handoff."""

    @abstractmethod
    async def conversation(
        self,
        context: HumanAssistPlatformContext,
        event: ConversationEvent,
    ) -> PlatformOperationResult:
        """Deliver or normalize one conversation event in either direction."""

    @abstractmethod
    async def end_conversation(
        self,
        context: HumanAssistPlatformContext,
        event: EndConversationEvent,
    ) -> PlatformOperationResult:
        """Close/resolve the platform conversation from either end."""


class HumanAssistPlatformError(RuntimeError):
    """Safe integration-boundary failure surfaced through the orchestrator."""

    def __init__(
        self,
        message: str,
        *,
        conversation: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.conversation = conversation


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
]
