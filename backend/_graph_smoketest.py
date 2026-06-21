"""One-off: drive the real negotiation loop with the browser layer stubbed, so
we emit genuine Arize traces (manager + worker graph nodes under a round span)
without needing Browserbase/Facebook. Run: ../.venv/bin/python _graph_smoketest.py
"""
from __future__ import annotations

import asyncio

# Tracing must initialize (and instrument LangChain + set the global tracer
# provider) BEFORE orchestration imports/creates the model.
import instrumentation  # noqa: F401

import orchestration
from orchestration import launch_negotiation

# Keep it short so we don't fan out 8 rounds x 2 workers of real LLM calls.
orchestration.MAX_NEGOTIATION_ROUNDS = 2

_SELLER_REPLIES = [
    "Hi, yeah it's still available. Price is $300, pretty firm though.",
    "I could maybe do $280 if you can pick up today.",
    "Alright, $260 cash and it's yours if you come by tonight.",
]


async def _fake_send_listing_message(context_id, message, *, listing_url=None, reply_wait_ms=0):
    # Rotate canned seller replies so workers keep negotiating.
    idx = _fake_send_listing_message.calls % len(_SELLER_REPLIES)
    _fake_send_listing_message.calls += 1
    print(f"  [stub-send] ctx={context_id} -> {message[:60]!r}")
    return {"reply_message": _SELLER_REPLIES[idx]}


_fake_send_listing_message.calls = 0


async def _fake_poll_listing_reply(context_id, *, sent_message=None, listing_url=None):
    return None  # no out-of-band late replies in the smoke test


async def _fake_close_active_listing_browser(context_id, *, listing_url=None):
    return True


async def _fake_close_all_active_listing_sessions():
    return 0


# Patch the names as imported into the orchestration module namespace.
orchestration.send_listing_message = _fake_send_listing_message
orchestration.poll_listing_reply = _fake_poll_listing_reply
orchestration.close_active_listing_browser = _fake_close_active_listing_browser
orchestration.close_all_active_listing_sessions = _fake_close_all_active_listing_sessions


ASSIGNMENTS = [
    {
        "worker_name": "smoketest_worker_1",
        "stagehand_context_id": "ctx_smoke_1",
        "listing_url": "https://www.facebook.com/marketplace/item/smoke1",
        "product": {
            "title": "iPhone 12 128GB",
            "price": "$300",
            "seller_name": "Alex",
            "description": "Good condition, minor scratches.",
            "reason_for_selling": "Upgrading to a new phone",
            "date_listed": "3 days ago",
        },
        "opener_reply": "Hi, it's still available. Asking $300.",
    },
    {
        "worker_name": "smoketest_worker_2",
        "stagehand_context_id": "ctx_smoke_2",
        "listing_url": "https://www.facebook.com/marketplace/item/smoke2",
        "product": {
            "title": "iPhone 12 64GB",
            "price": "$280",
            "seller_name": "Sam",
            "description": "Great shape, comes with case.",
            "reason_for_selling": "Moving, need it gone this week",
            "date_listed": "just listed today",
        },
        "opener_reply": "Still available! $280 and it's a great deal.",
    },
]


async def main() -> None:
    print("[smoketest] launching negotiation (2 workers, 2 rounds)...")
    states = await launch_negotiation(ASSIGNMENTS, event_publisher=None)
    for s in states:
        print(
            f"[smoketest] {s['worker_id']}: rounds={s.get('negotiation_round')} "
            f"success={s.get('success')} last_mode={s.get('manager_mode')}"
        )
    # Force the BatchSpanProcessor to flush before the script exits, else spans
    # never reach Arize.
    tp = instrumentation.tracer_provider
    if tp is not None:
        print("[smoketest] flushing spans to Arize...")
        tp.force_flush()
        tp.shutdown()
    print("[smoketest] done.")


if __name__ == "__main__":
    asyncio.run(main())
