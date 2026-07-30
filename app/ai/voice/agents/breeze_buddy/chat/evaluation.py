"""Evaluation-only transcript projection for Breeze Buddy chat."""

from typing import Any, Dict, Iterable, List

from app.schemas.breeze_buddy.chat import ChatMessage


def messages_for_ai_evaluation(
    messages: Iterable[ChatMessage],
) -> List[Dict[str, Any]]:
    """Return customer/Buddy prose without human or lifecycle messages."""
    return [
        {"role": message.role.value, "content": message.content}
        for message in messages
        if message.content
        and message.sender_type not in {"human", "system", "internal"}
    ]


__all__ = ["messages_for_ai_evaluation"]
