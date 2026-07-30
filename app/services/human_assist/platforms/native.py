"""Native Loom Inbox implementation of the three-operation platform contract."""

from app.services.human_assist.platforms.base import (
    ConversationEvent,
    ConversationSource,
    EndConversationEvent,
    HandoffEvent,
    HumanAssistPlatform,
    HumanAssistPlatformContext,
    HumanAssistPlatformSpec,
    PlatformMessage,
    PlatformOperationResult,
)


class NativeHumanAssistPlatform(HumanAssistPlatform):
    spec = HumanAssistPlatformSpec(
        key="native",
        display_name="Native Inbox",
        description="Handle the conversation directly from the Loom Inbox.",
    )

    async def handoff(
        self,
        context: HumanAssistPlatformContext,
        event: HandoffEvent,
    ) -> PlatformOperationResult:
        return PlatformOperationResult(conversation_ref=context.conversation_id)

    async def conversation(
        self,
        context: HumanAssistPlatformContext,
        event: ConversationEvent,
    ) -> PlatformOperationResult:
        if not event.content:
            return PlatformOperationResult()
        sender_type = (
            "customer" if event.source == ConversationSource.CUSTOMER else "human"
        )
        return PlatformOperationResult(
            messages=(
                PlatformMessage(
                    content=event.content,
                    sender_type=sender_type,
                    actor_id=event.actor_id,
                ),
            )
        )

    async def end_conversation(
        self,
        context: HumanAssistPlatformContext,
        event: EndConversationEvent,
    ) -> PlatformOperationResult:
        return PlatformOperationResult()


__all__ = ["NativeHumanAssistPlatform"]
