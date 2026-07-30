"""Best-effort realtime notifications for durable Human Assist changes.

PostgreSQL remains authoritative. Redis pub/sub only wakes connected widget
and Inbox SSE streams so they can re-read committed state without polling.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Optional

from app.core.logger import logger
from app.schemas.breeze_buddy.human_assist import HumanAssistConversation
from app.services.redis.client import get_redis_service

HUMAN_ASSIST_INBOX_CHANNEL = "breeze-buddy:human-assist:inbox"
_HUMAN_ASSIST_SESSION_CHANNEL_PREFIX = "breeze-buddy:human-assist:session:"


def human_assist_session_channel(chat_session_id: str) -> str:
    return f"{_HUMAN_ASSIST_SESSION_CHANNEL_PREFIX}{chat_session_id}"


async def publish_human_assist_event(
    conversation: HumanAssistConversation,
    *,
    kind: str,
) -> None:
    """Notify both merchant and customer streams after a committed mutation."""
    payload = json.dumps(
        {
            "kind": kind,
            "conversation_id": conversation.id,
            "chat_session_id": conversation.chat_session_id,
            "reseller_id": conversation.reseller_id,
            "merchant_id": conversation.merchant_id,
            "status": conversation.status.value,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "close_reason": (
                conversation.close_reason.value if conversation.close_reason else None
            ),
        }
    )
    try:
        redis = await get_redis_service()
        client = await redis.get_client()
        await client.publish(HUMAN_ASSIST_INBOX_CHANNEL, payload)  # type: ignore[union-attr]
        await client.publish(  # type: ignore[union-attr]
            human_assist_session_channel(conversation.chat_session_id),
            payload,
        )
    except Exception as exc:
        # Losing a wake-up must never roll back or fail the durable operation.
        # SSE clients reconnect and re-read PostgreSQL to recover missed state.
        logger.warning(
            "Human Assist realtime publish failed: "
            f"conversation={conversation.id} kind={kind} error={exc}"
        )


async def subscribe_human_assist_events(
    channel: str,
    *,
    keepalive_seconds: float = 15.0,
) -> AsyncIterator[Optional[Dict[str, Any]]]:
    """Yield decoded pub/sub events, or ``None`` when an SSE ping is due."""
    redis = await get_redis_service()
    client = await redis.get_client()
    pubsub = client.pubsub()  # type: ignore[union-attr]
    await pubsub.subscribe(channel)
    try:
        # Lets callers take their initial PostgreSQL snapshot only after the
        # subscription exists, closing the snapshot/subscription race.
        yield {"kind": "ready"}
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=keepalive_seconds,
            )
            if message is None:
                yield None
                continue
            data = message.get("data")
            if isinstance(data, bytes):
                data = data.decode("utf-8", errors="replace")
            try:
                decoded = json.loads(str(data))
            except (TypeError, ValueError):
                logger.warning(
                    f"Human Assist realtime event was not valid JSON on {channel}"
                )
                continue
            if isinstance(decoded, dict):
                yield decoded
    finally:
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        except Exception:
            pass


__all__ = [
    "HUMAN_ASSIST_INBOX_CHANNEL",
    "human_assist_session_channel",
    "publish_human_assist_event",
    "subscribe_human_assist_events",
]
