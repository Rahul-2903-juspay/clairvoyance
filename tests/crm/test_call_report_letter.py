"""A call report answers its square but never becomes the run's latest
letter (22 Sep 2026; docs/crm/runbooks/after-call.md).

A wait square after a call hears that call's `call.completed` (matched on
the lead the call square queued) and branches on the outcome — today's
words, nothing new. What was wrong: every heard letter took the
latest-letter pointer, so the NEXT call was built from the report's facts
and the offers the merchant sent before the first call were gone from the
second (seen live 16 Sep 2026). The report now keeps its facts under its
square and leaves the pointer where the merchant's last word put it.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from uuid import uuid4

import app.crm.outreach.entry as entry
from app.ai.voice.agents.breeze_buddy.crm_mirror import MIRRORS
from app.crm.outreach.nodes.context import run_facts
from app.crm.outreach.plans import validate_definition
from app.crm.outreach.schemas import EnrollmentRun, WorkflowNode
from app.crm.record.contracts import CALL_REPORT_SOURCES, RawEvent
from tests.crm.test_plan_templates import PLANS, _load

NOW = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)

AFTER_CALL = WorkflowNode.model_validate(
    {
        "id": "after-call-1",
        "type": "wait",
        "topics": ["call.completed"],
        "key": "outcome",
        "minutes": 1440,
        "match": {"payload": "lead_id", "run": "lead_call-1"},
    }
)
QUIET = WorkflowNode.model_validate(
    {"id": "quiet-30m", "type": "wait", "topics": ["OFFERED"], "key": "$topic"}
)


def _event(topic: str, payload: Dict[str, Any], source: str = "flipkart") -> RawEvent:
    return RawEvent(
        id="ev-1",
        merchant_id="m1",
        source=source,
        topic=topic,
        schema_version="1",
        external_id=f"{topic}:ev-1",
        payload=payload,
        received_at=NOW,
        occurred_at=NOW,
    )


def _run(node: str, context: Dict[str, Any]) -> EnrollmentRun:
    return EnrollmentRun(
        id=uuid4(),
        merchant_id="m1",
        workflow_id=uuid4(),
        workflow_version=1,
        customer_id=uuid4(),
        status="waiting",
        current_node=node,
        wake_at=NOW + timedelta(minutes=30),
        entered_at=NOW - timedelta(hours=1),
        exited_at=None,
        exit_reason=None,
        context=context,
        enrollment_key="c-1",
        attempts=1,
        last_error=None,
        node_arrived_at=NOW - timedelta(minutes=20),
    )


def test_the_call_report_sources_are_the_mirrors_call_sources() -> None:
    """Pinned to buddy's mirror, not to a literal: rename the source there and
    a literal would stay green while reports quietly took the pointer again
    (the 16 Sep bug). lead.pushed is the mirror's too, but it is the lead
    API's word — not a call report — and must stay out."""
    assert CALL_REPORT_SOURCES == {
        source for topic, source in MIRRORS.items() if topic.startswith("call.")
    }
    assert MIRRORS["lead.pushed"] not in CALL_REPORT_SOURCES


def test_a_call_report_answers_the_square_but_never_takes_the_latest_letter() -> None:
    report = _event(
        "call.completed", {"lead_id": "lead-1", "outcome": "NO_ANSWER"}, "telephony"
    )
    assert entry._reply_patch(AFTER_CALL, report, "NO_ANSWER") == {
        "reply_after-call-1": "NO_ANSWER",
        "cut_short_by": "ev-1",
    }
    letter = _event("OFFERED", {"customer_id": "c-1"})
    assert entry._reply_patch(QUIET, letter, "OFFERED") == {
        "reply_quiet-30m": "OFFERED",
        "cut_short_by": "ev-1",
        "latest_letter": "quiet-30m",
    }


def test_the_after_call_square_hears_only_the_call_it_queued() -> None:
    """The call square writes lead_<square> into context; the report carries
    lead_id; `match` compares them as text. A lead id is uuid5 of run, square
    and visit, so no other run, square or visit can answer to it."""
    run = _run("after-call-1", {"customer_id": "c-1", "lead_call-1": "lead-1"})
    mine = _event(
        "call.completed", {"lead_id": "lead-1", "outcome": "BUSY"}, "telephony"
    )
    other_call = _event("call.completed", {"lead_id": "lead-2"}, "telephony")
    no_lead = _event("call.completed", {"outcome": "BUSY"}, "telephony")
    assert entry._is_about(AFTER_CALL, mine, run)
    assert not entry._is_about(AFTER_CALL, other_call, run)
    assert not entry._is_about(AFTER_CALL, no_lead, run)
    assert entry._answer_for(AFTER_CALL, mine) == "BUSY"


def test_the_next_call_speaks_the_merchants_facts_not_the_reports() -> None:
    """What the fix changes for call-2's payload: the merchant's letter on
    quiet-30m stays the latest word; the report sits under its own square."""
    letter_facts = {"offers": "1. Bank: 12 months", "customer_id": "c-1"}
    report_facts = {"outcome": "NO_ANSWER", "lead_id": "lead-1"}
    context = {
        "customer_id": "c-1",
        "phone": "+919876543210",
        "latest_letter": "quiet-30m",  # the letter's pointer, left alone by the report
        "facts": {"quiet-30m": letter_facts, "after-call-1": report_facts},
    }
    facts = run_facts(context)
    assert facts["offers"] == "1. Bank: 12 months"
    assert "outcome" not in facts  # the report's scalars are not the merchant's
    assert facts["facts_after-call-1_outcome"] == "NO_ANSWER"  # but readable by name
    # had the report taken the pointer (the old behaviour), call-2 would have
    # spoken the outcome and lost nothing of the offers only by luck of naming
    taken = {**context, "latest_letter": "after-call-1"}
    assert run_facts(taken)["outcome"] == "NO_ANSWER"


def test_the_example_plan_waits_for_each_calls_own_report() -> None:
    plan = _load(PLANS / "line-nudge-call-wait.json")
    assert validate_definition(plan) == []
    squares = {n["id"]: n for n in plan["nodes"]}
    for i in ("1", "2"):
        after = squares[f"after-call-{i}"]
        assert after["topics"] == ["call.completed"] and after["key"] == "outcome"
        assert after["match"] == {"payload": "lead_id", "run": f"lead_call-{i}"}
        assert after["minutes"] == 1440 and "window" not in after
        assert [e[0] for e in plan["edges"] if e[1] == f"after-call-{i}"] == [
            f"call-{i}"
        ]
        labels = sorted(e[2] for e in plan["edges"] if e[0] == f"after-call-{i}")
        assert labels == ["else", "timeout"]
    # the call squares are today's: no waiting words on them
    for i in ("1", "2"):
        assert set(squares[f"call-{i}"]) == {"id", "type", "template_id"}


# --- a late report never takes the refresh path either --------------------

_TWO_CALLS = {
    "entry": {"topic": "OFFERED"},
    "goals": [{"topics": ["GRANTED"]}],
    "nodes": [
        {"id": "quiet-30m", "type": "wait", "topics": ["OFFERED"], "key": "$topic"},
        {"id": "call-1", "type": "call", "template_id": "tpl-1"},
        {
            "id": "after-call-1",
            "type": "wait",
            "topics": ["call.completed"],
            "key": "outcome",
            "minutes": 1440,
            "match": {"payload": "lead_id", "run": "lead_call-1"},
        },
        {"id": "call-2", "type": "call", "template_id": "tpl-1"},
    ],
    "edges": [
        ["quiet-30m", "call-1", "else"],
        ["call-1", "after-call-1"],
        ["after-call-1", "call-2", "else"],
        ["after-call-1", "call-2", "timeout"],
    ],
}


class _Accessor:
    def __init__(self) -> None:
        self.resumes: list = []
        self.refreshes: list = []

    async def resume_run_by_id(self, _m, _r, node_id, patch, facts=None) -> bool:
        # The SQL's own condition: only the square the run stands on.
        self.resumes.append(node_id)
        return False

    async def refresh_run_facts(self, _m, _r, node_id, facts, cut_short_by=None):
        self.refreshes.append((node_id, facts))
        return True


async def _wake_parked_on_call_2(monkeypatch, event: RawEvent) -> _Accessor:
    from app.crm.outreach.schemas import WorkflowDefinition

    accessor = _Accessor()
    monkeypatch.setattr(
        entry.enrollment_accessor, "resume_run_by_id", accessor.resume_run_by_id
    )
    monkeypatch.setattr(
        entry.enrollment_accessor, "refresh_run_facts", accessor.refresh_run_facts
    )
    definition = WorkflowDefinition.model_validate(_TWO_CALLS)
    run = _run("call-2", {"lead_call-1": "lead-1", "offers": "1. Axis"})
    await entry._wake_on_reply(run, definition, event)
    return accessor


async def test_a_late_report_on_a_parked_run_refreshes_nothing(monkeypatch) -> None:
    """The second path a letter writes to the run. call-1's report arrives
    after the 24h backstop moved the run on and call-2 parked: after-call-1
    no longer holds the token, so the resume matches no row — and the
    non-listening fallback must not then write the report's `outcome` at the
    TOP level (the next call would speak it) or re-arm the run on it."""
    report = _event(
        "call.completed", {"lead_id": "lead-1", "outcome": "NO_ANSWER"}, "telephony"
    )

    accessor = await _wake_parked_on_call_2(monkeypatch, report)

    assert accessor.resumes == ["after-call-1"], "its own square is still asked"
    assert accessor.refreshes == [], "and nothing is refreshed at the top level"


async def test_a_merchant_letter_on_a_parked_run_still_refreshes(monkeypatch) -> None:
    """The fallback keeps doing its job for the producer's own letters."""
    letter = _event("OFFERED", {"offers": "1. HDFC"})

    accessor = await _wake_parked_on_call_2(monkeypatch, letter)

    ((node_id, facts),) = accessor.refreshes
    assert node_id == "call-2" and facts["offers"] == "1. HDFC"


async def test_a_goal_ends_the_run_while_it_waits_on_after_call(monkeypatch) -> None:
    """The guarantee this shape leans on. `after-call` hears only its call's
    report — but a goal is judged for every open run BEFORE any square is
    asked, wherever the run stands. A customer who orders while the phone is
    ringing ends the run as goal_met; nothing is dropped."""
    from tests.crm.test_workflow_entry import _install, _Spine

    run = _run("after-call-1", {"lead_call-1": "lead-1"})
    spine = _Spine(flows=[], runs=[run], versions={(run.workflow_id, 1): _TWO_CALLS})
    _install(monkeypatch, spine)

    await entry.consume_attributed_event(_event("GRANTED", {}), "c-1", {})

    assert [(r, reason) for r, reason, *_ in spine.cancels] == [
        (str(run.id), "goal_met")
    ]
    assert spine.resumes == [] and spine.refreshes == [], "ended, so nothing wakes"
