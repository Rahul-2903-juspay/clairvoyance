"""RBAC-gated Human Assist inbox and platform endpoints."""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from app.ai.voice.agents.breeze_buddy.chat.sse import SSEEvent, format_sse
from app.api.routers.breeze_buddy.templates.rbac import validate_template_access
from app.api.security.breeze_buddy.rbac_token import get_current_user_with_rbac
from app.core.security.authorization import require_role
from app.database.accessor.breeze_buddy.human_assist import (
    get_human_assist_conversation,
    get_human_assist_scope_signature,
    list_human_assist_conversations,
    list_human_assist_transcript,
)
from app.schemas import UserInfo, UserRole
from app.schemas.breeze_buddy.human_assist import (
    HumanAssistConversation,
    HumanAssistConversationDetail,
    HumanAssistConversationList,
    HumanAssistHumanMessageRequest,
    HumanAssistPlatformInfo,
    HumanAssistPlatformList,
    HumanAssistStatus,
    HumanAssistStatusCounts,
)
from app.services.human_assist import (
    HumanAssistBusyError,
    append_human_human_assist_message,
    claim_human_assist,
    close_human_assist,
)
from app.services.human_assist.events import (
    HUMAN_ASSIST_INBOX_CHANNEL,
    subscribe_human_assist_events,
)
from app.services.human_assist.platforms import list_platforms

router = APIRouter(prefix="/human-assist", tags=["human-assist"])
_ASSIST_ROLES = [
    UserRole.ADMIN,
    UserRole.RESELLER,
    UserRole.MERCHANT,
    UserRole.USER,
]


def _agent_id(user: UserInfo) -> str:
    return str(getattr(user, "id", None) or user.username)


def _validate_access(user: UserInfo, conversation: HumanAssistConversation) -> None:
    validate_template_access(
        user,
        conversation.reseller_id,
        conversation.merchant_id,
        operation="access Human Assist conversation",
    )


async def _conversation_or_404(
    conversation_id: str,
    user: UserInfo,
    *,
    include_stats: bool = False,
) -> HumanAssistConversation:
    conversation = await get_human_assist_conversation(
        conversation_id,
        include_stats=include_stats,
    )
    if conversation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Human Assist conversation not found",
        )
    _validate_access(user, conversation)
    return conversation


def _resolve_list_scope(
    current_user: UserInfo,
    reseller_id: Optional[str],
    merchant_id: Optional[str],
) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    reseller_ids: Optional[List[str]] = None
    merchant_ids: Optional[List[str]] = None
    if current_user.role != "admin":
        if reseller_id:
            validate_template_access(
                current_user,
                reseller_id,
                merchant_id,
                operation="list Human Assist conversations",
            )
        elif merchant_id and (
            merchant_id not in current_user.merchant_ids
            and "*" not in current_user.merchant_ids
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied to merchant {merchant_id}",
            )
        if not reseller_id and "*" not in current_user.reseller_ids:
            reseller_ids = current_user.reseller_ids
        if not merchant_id and "*" not in current_user.merchant_ids:
            merchant_ids = current_user.merchant_ids
    return reseller_ids, merchant_ids


def _event_matches_scope(
    event: Dict[str, Any],
    *,
    reseller_id: Optional[str],
    merchant_id: Optional[str],
    reseller_ids: Optional[List[str]],
    merchant_ids: Optional[List[str]],
) -> bool:
    event_reseller_id = event.get("reseller_id")
    event_merchant_id = event.get("merchant_id")
    if reseller_id:
        if event_reseller_id != reseller_id:
            return False
    elif reseller_ids is not None and event_reseller_id not in reseller_ids:
        return False
    if merchant_id:
        if event_merchant_id != merchant_id:
            return False
    elif merchant_ids is not None and event_merchant_id not in merchant_ids:
        return False
    return True


@router.get("/platforms", response_model=HumanAssistPlatformList)
async def get_platforms(
    _: UserInfo = Depends(get_current_user_with_rbac),
) -> HumanAssistPlatformList:
    """Return registered adapters for the merchant platform switcher."""
    return HumanAssistPlatformList(
        platforms=[
            HumanAssistPlatformInfo(
                key=platform.key,
                display_name=platform.display_name,
                description=platform.description,
            )
            for platform in list_platforms()
        ]
    )


@router.get("/conversations", response_model=HumanAssistConversationList)
async def list_conversations(
    statuses: Optional[List[HumanAssistStatus]] = Query(default=None, alias="status"),
    reseller_id: Optional[str] = Query(default=None),
    merchant_id: Optional[str] = Query(default=None),
    search: Optional[str] = Query(
        default=None,
        max_length=200,
        description="Match a Human Assist ID fragment",
    ),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=30, ge=1, le=100),
    current_user: UserInfo = Depends(get_current_user_with_rbac),
) -> HumanAssistConversationList:
    reseller_ids, merchant_ids = _resolve_list_scope(
        current_user,
        reseller_id,
        merchant_id,
    )

    conversations, total, counts = await list_human_assist_conversations(
        statuses=[item.value for item in statuses] if statuses else None,
        reseller_id=reseller_id,
        merchant_id=merchant_id,
        reseller_ids=reseller_ids,
        merchant_ids=merchant_ids,
        search=(search or "").strip() or None,
        limit=limit,
        offset=(page - 1) * limit,
    )
    return HumanAssistConversationList(
        conversations=conversations,
        total=total,
        active_total=counts["active_total"],
        counts=HumanAssistStatusCounts(
            pending=counts["pending_total"],
            open=counts["open_total"],
            closed=counts["closed_total"],
            timed_out=counts["timed_out_total"],
            active=counts["active_total"],
        ),
        page=page,
        limit=limit,
    )


@router.get("/stream", summary="Stream scoped Human Assist Inbox changes")
async def stream_conversations(
    request: Request,
    reseller_id: Optional[str] = Query(default=None),
    merchant_id: Optional[str] = Query(default=None),
    current_user: UserInfo = Depends(get_current_user_with_rbac),
) -> StreamingResponse:
    """Wake the Inbox after durable changes visible in its current RBAC scope."""
    reseller_ids, merchant_ids = _resolve_list_scope(
        current_user,
        reseller_id,
        merchant_id,
    )

    async def _stream():
        last_signature = await get_human_assist_scope_signature(
            reseller_id=reseller_id,
            merchant_id=merchant_id,
            reseller_ids=reseller_ids,
            merchant_ids=merchant_ids,
        )
        next_signature_check = time.monotonic() + 15.0
        async for event in subscribe_human_assist_events(HUMAN_ASSIST_INBOX_CHANNEL):
            if await request.is_disconnected():
                break
            if event is not None and event.get("kind") == "ready":
                yield format_sse(SSEEvent(event="human_assist_ready"))
                continue
            if event is not None and _event_matches_scope(
                event,
                reseller_id=reseller_id,
                merchant_id=merchant_id,
                reseller_ids=reseller_ids,
                merchant_ids=merchant_ids,
            ):
                latest_activity, ticket_count = last_signature
                try:
                    occurred_at = datetime.fromisoformat(
                        str(event.get("occurred_at")).replace("Z", "+00:00")
                    )
                except ValueError:
                    occurred_at = datetime.now(timezone.utc)
                last_signature = (
                    max(latest_activity or occurred_at, occurred_at),
                    ticket_count + (1 if event.get("kind") == "handoff" else 0),
                )
                next_signature_check = time.monotonic() + 15.0
                yield format_sse(
                    SSEEvent(
                        event="human_assist_ticket",
                        data={
                            "kind": event.get("kind"),
                            "conversation_id": event.get("conversation_id"),
                            "status": event.get("status"),
                            "close_reason": event.get("close_reason"),
                        },
                    )
                )
                continue

            now = time.monotonic()
            if event is None or now >= next_signature_check:
                signature = await get_human_assist_scope_signature(
                    reseller_id=reseller_id,
                    merchant_id=merchant_id,
                    reseller_ids=reseller_ids,
                    merchant_ids=merchant_ids,
                )
                next_signature_check = now + 15.0
                latest_activity, ticket_count = signature
                known_activity, known_count = last_signature
                if ticket_count != known_count or (
                    latest_activity is not None
                    and (known_activity is None or latest_activity > known_activity)
                ):
                    last_signature = signature
                    yield format_sse(SSEEvent(event="human_assist_sync"))
                elif event is None:
                    yield ": keep-alive\n\n"

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get(
    "/conversations/{conversation_id}",
    response_model=HumanAssistConversationDetail,
)
async def get_conversation(
    conversation_id: str,
    after_idx: Optional[int] = Query(
        default=None,
        ge=-1,
        description=(
            "Return only messages newer than this idx. The Inbox passes the "
            "last idx it holds so a realtime refresh transfers the tail, not "
            "the whole transcript."
        ),
    ),
    current_user: UserInfo = Depends(get_current_user_with_rbac),
) -> HumanAssistConversationDetail:
    conversation = await _conversation_or_404(
        conversation_id,
        current_user,
        include_stats=True,
    )
    messages = await list_human_assist_transcript(
        conversation.chat_session_id,
        after_idx=after_idx,
    )
    return HumanAssistConversationDetail(
        conversation=conversation,
        messages=messages,
        incremental=after_idx is not None,
    )


@router.post(
    "/conversations/{conversation_id}/open",
    response_model=HumanAssistConversation,
)
async def open_conversation(
    conversation_id: str,
    current_user: UserInfo = Depends(get_current_user_with_rbac),
) -> HumanAssistConversation:
    existing = await _conversation_or_404(conversation_id, current_user)
    require_role(current_user, _ASSIST_ROLES)
    agent_id = _agent_id(current_user)
    if existing.status == HumanAssistStatus.OPEN and existing.opened_by == agent_id:
        return existing
    try:
        claimed = await claim_human_assist(conversation_id, agent_id)
    except HumanAssistBusyError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    if claimed is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ticket was already opened, closed, or its claim deadline passed",
        )
    return claimed


@router.post(
    "/conversations/{conversation_id}/messages",
    response_model=dict,
    status_code=status.HTTP_201_CREATED,
)
async def send_human_message(
    conversation_id: str,
    body: HumanAssistHumanMessageRequest,
    current_user: UserInfo = Depends(get_current_user_with_rbac),
) -> dict:
    await _conversation_or_404(conversation_id, current_user)
    require_role(current_user, _ASSIST_ROLES)
    try:
        message = await append_human_human_assist_message(
            conversation_id,
            agent_id=_agent_id(current_user),
            content=body.content.strip(),
        )
    except HumanAssistBusyError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "human_assist_busy",
                "message": str(exc),
            },
        ) from exc
    if message is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Open and own this ticket before replying",
        )
    return {"message": message.model_dump(mode="json")}


@router.post(
    "/conversations/{conversation_id}/close",
    response_model=HumanAssistConversation,
)
async def close_conversation(
    conversation_id: str,
    current_user: UserInfo = Depends(get_current_user_with_rbac),
) -> HumanAssistConversation:
    existing = await _conversation_or_404(conversation_id, current_user)
    require_role(current_user, _ASSIST_ROLES)
    if existing.status not in (HumanAssistStatus.PENDING, HumanAssistStatus.OPEN):
        return existing
    closed = await close_human_assist(
        conversation_id,
        closed_by=_agent_id(current_user),
    )
    if closed is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ticket state changed; refresh the inbox",
        )
    return closed


__all__ = ["router"]
