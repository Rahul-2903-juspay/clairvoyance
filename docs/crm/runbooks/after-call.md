# The wait after a call — one queued call per run

*22 Sep 2026.* A call square queues its lead and moves on at once, so the
timer after it counts from the insert, not from the call. A run held
overnight queues its morning call, waits its gap, and queues the next
while the first is still in the dialler's queue. On a 6,000-run morning
that is a second lead per customer behind a pile that already takes an
hour to drain — seen on 23 Sep 2026 as a second wave of inserts exactly 30
minutes after the first.

## The shape

Nothing new in the engine: a listening wait after the call, hearing the
call's own report.

```json
{"id": "call-1", "type": "call", "template_id": "..."},
{"id": "after-call-1", "type": "wait",
 "topics": ["call.completed"], "key": "outcome", "minutes": 1440,
 "match": {"payload": "lead_id", "run": "lead_call-1"}}
```
with arrows `after-call-1 → gap-30m` labelled `else` and `timeout`.

- **The call square is today's.** It queues the lead, writes the lead id
  into the run's context as `lead_call-1`, and the run walks on to
  `after-call-1` in the same visit.
- **`after-call-1` waits for that call's report.** The telephony mirror
  writes a `call.completed` when the call ends, carrying `lead_id` and
  `outcome`. `match` compares the report's `lead_id` with the run's
  `lead_call-1` as text. A lead id is uuid5 of run, square and visit, so
  no other run, square or visit answers to it — two applications on one
  phone never hear each other's call, and a late report from an earlier
  call wakes nothing.
- **It branches on the outcome.** `key: outcome` makes the outcome the
  square's answer, so an author may draw `NO_ANSWER`, `BUSY`, `INTERESTED`
  arrows; `else` takes the rest, `timeout` fires when no report came in
  `minutes`. The example plan sends both to the gap.
- **The backstop must outlast the pile.** `minutes` is how long a queued
  lead may wait for a line before the run gives up on the report. Shorter
  than the dialler's drain time and runs time out of `after-call` while
  their lead is still queued — and queue the next call behind it, the
  very thing this shape prevents. 1440 in the example.
- **It listens for nothing else.** No merchant topics and no window on
  `after-call`. One `match` per square is the reason: merchant letters
  match on the customer, the report on the lead.
- **A merchant letter that arrives while the run waits on `after-call` is
  lost.** It is not heard later at the gap: a letter wakes only the square
  the run stands on, `after-call` does not hear merchant topics, and the
  fallback for a square that listens to nothing does not apply to a
  square that listens. So a `LINE_OFFERED` or `LINE_KYC_COMPLETED` sent
  during the wait does not update the run's facts, and the next call
  speaks the offers from before it. The wait is not just the ring: it runs
  from queueing the call until its report arrives — up to about 80 minutes
  for the morning pile at 77 calls a minute. Goals still end the run: a
  goal is judged for every open run before any square is asked, wherever
  the run stands — a customer who orders mid-call ends it as `goal_met`
  (pinned by `test_a_goal_ends_the_run_while_it_waits_on_after_call`).
  Putting the merchant topics on `after-call` is not the fix — re-routing
  from there queues a second call while the first is still queued, the
  thing this shape prevents.

  *Accepted, 23 Sep 2026.* Recording such a letter's facts without waking
  the run was considered and declined: it would need a write that cannot
  fence an in-flight walker visit (waking the run on `after-call` would
  take its `timeout` arrow and queue the next call early), for letters that
  carry data but never end a run.

## What this change fixes

Every heard letter used to take the run's latest-letter pointer. A
report is a letter, so after `after-call-1` heard it the pointer named
that square, and the next call's payload — built from "the latest
letter's facts" — spoke the report: its outcome and lead id, with the
offers the merchant sent before the first call gone from the second (seen
live 16 Sep 2026).

`CALL_REPORT_SOURCES` (the record module's surface; today `telephony`)
names the sources whose letters are our own call reports. A report from
one of them answers its square and keeps its facts under it —
`facts_after-call-1_outcome` is readable by name — but never takes the
pointer. Where it lives: `entry.py::_reply_patch`, the one place a heard
letter's context patch is built — and the non-listening fallback in
`_wake_on_reply` skips reports outright, so a late report on a run parked
on a later call writes nothing at the top level and re-arms nothing.

## Rolling it out

1. Deploy api, event-worker, walker. **Open cart runs change at deploy.**
   `cart-recovery-retry` and `cart-recovery-fallback` already have an
   `after-call` square hearing `call.completed`. Today its report takes the
   pointer, so the next rescue call's lead carries the report's `outcome`
   (and whatever else it declares) at the top level; after this deploy it
   does not — it is under `facts_after-call_outcome` instead. **Before
   deploying, check that no live cart call template uses `{outcome}`**: one
   that does would speak the placeholder. Other plans change nothing.
2. Set the dialler template's `max_retry` to 0 for workflow templates: the
   ladder is the retry, and a dialler retry is a fresh lead no square is
   waiting for. This is the only thing keeping the double queue away — with
   a retry left on, attempt 1's report moves the run on while Buddy's retry
   lead is still queued.
   - [ ] every template a published plan's call square names has
     `max_retry` = 0 (checked by: ____)
3. Publish the plan with an `after-call` square behind each call and
   `on_publish: migrate`. Runs already waiting keep their square and
   their alarm; the next call square they reach is followed by its wait.
   Publish after the morning sweep, not during it.

## Rolling back

Nothing to move. The words are today's; old code hears the report the
same way and only differs in taking the pointer again.
