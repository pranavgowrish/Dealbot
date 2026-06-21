"""
Manager-led negotiation loop for Dealbot workers.

Flow:
  manager assigns roles and round directives across workers
  -> each worker evaluates latest seller reply
  -> manager-guided send (push/close/stall/probe)
  -> repeat until success or max rounds.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
from typing import Any, TypedDict

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, SystemMessage

from agent_events import AgentEventPublisher
from browserb import (
    close_active_listing_browser,
    poll_listing_reply,
    send_listing_message,
)
from memory import AgentMemoryStore, parse_price


load_dotenv()

model = init_chat_model("claude-sonnet-4-6", temperature=0.0)
memory = AgentMemoryStore()

MAX_NEGOTIATION_ROUNDS = 8
REPLY_WAIT_MS = int(os.environ.get("REPLY_WAIT_MS", "1200"))
REPLY_POLL_INTERVAL_MS = int(os.environ.get("REPLY_POLL_INTERVAL_MS", "2500"))
TACTIC_CHOICES = (
    "anchor_low",
    "bundle",
    "flinch",
    "walk_away",
    "deadline_cash_pickup",
)
MANAGER_ROLE_CHOICES = ("aggressive_anchor", "closer")
MANAGER_MODE_CHOICES = ("probe_floor", "push_hard", "close_now", "stall", "pull_off")
TACTIC_PLAYBOOK: dict[str, str] = {
    "anchor_low": (
        "Set a believable lower anchor tied to condition/comps, then make one concrete "
        "price ask."
    ),
    "bundle": (
        "Use a bundle offer only when there is a plausible add-on (stand, accessory, "
        "delivery, spare part) and give one all-in number."
    ),
    "flinch": (
        "Show mild budget pressure first (real-human flinch), then follow with your "
        "price ask."
    ),
    "walk_away": (
        "Politely signal you can pass and pursue another listing; keep tone respectful "
        "and non-combative."
    ),
    "deadline_cash_pickup": (
        "Use the cash-and-pickup-now lever with a specific near-term pickup window "
        "today/tonight."
    ),
}


def _emit_event(
    event_publisher: AgentEventPublisher | None,
    *,
    event_type: str,
    actor_type: str,
    actor_id: str,
    summary: str,
    details: dict[str, Any] | None = None,
) -> None:
    if event_publisher is None:
        return
    event_publisher.publish(
        event_type,
        actor_type=actor_type,
        actor_id=actor_id,
        summary=summary,
        details=details or {},
    )


class WorkerState(TypedDict):
    worker_id: str
    stagehand_context_id: str
    seller_name: str
    listing_title: str
    listing_description: str
    listing_posted_text: str
    listing_reason: str
    listing_url: str
    listed_price: float | None
    current_min_price: float
    target_budget: float
    max_price: float
    messages: list[AnyMessage]
    seller_budged: bool
    negotiation_round: int
    max_rounds: int
    success: bool
    last_sent: str | None
    last_reply: str | None
    last_tactic: str | None
    last_assessment: dict[str, str] | None
    manager_role: str
    manager_mode: str
    manager_instruction: str
    manager_focus_worker: str | None
    manager_round_brief: str | None
    awaiting_reply: bool
    needs_manager_review: bool


def _target_seller_label(state: WorkerState) -> str:
    seller = state.get("seller_name") or "the seller"
    title = state.get("listing_title") or "this listing"
    return f"{seller} ({title})"


def _success_from_reply(reply: str, target_budget: float) -> bool:
    lowered = reply.lower()
    success_phrases = (
        "deal",
        "sounds good",
        "works for me",
        "i can do",
        "okay",
        "ok ",
        "yes",
    )
    if not any(phrase in lowered for phrase in success_phrases):
        return False
    price = parse_price(reply)
    return price is not None and price <= target_budget


def _extract_json_object(raw_text: str) -> dict[str, Any] | None:
    text = str(raw_text).strip()
    if text.startswith("```"):
        text = "\n".join(
            line for line in text.splitlines() if not line.strip().startswith("```")
        ).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        parsed = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _estimate_listing_staleness(posted_text: str) -> str:
    normalized = " ".join((posted_text or "").lower().split())
    if not normalized:
        return "unknown"
    if "just listed" in normalized or "today" in normalized:
        return "fresh"
    match = re.search(r"(\d+)\s*(hour|day|week|month)s?", normalized)
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        if unit == "hour":
            return "fresh"
        if unit == "day":
            return "stale" if value >= 7 else ("mid" if value >= 3 else "fresh")
        if unit == "week":
            return "stale" if value >= 2 else "mid"
        if unit == "month":
            return "stale"
    if "price dropped" in normalized or "relisted" in normalized:
        return "stale"
    return "unknown"


def _fallback_tactic_assessment(
    state: WorkerState, existing_chat: list[AnyMessage]
) -> dict[str, str]:
    seller_replies = [
        str(message.content)
        for message in existing_chat
        if getattr(message, "type", "") == "human"
    ]
    combined_seller = " ".join(seller_replies).lower()
    listing_blob = " ".join(
        [
            state.get("listing_description", ""),
            state.get("listing_reason", ""),
            state.get("listing_posted_text", ""),
        ]
    ).lower()
    combined = f"{combined_seller} {listing_blob}".strip()

    firm_signals = ("firm", "final", "lowest i can do", "not negotiable", "price is set")
    flexible_signals = (
        "can do",
        "could do",
        "willing",
        "best i can",
        "make an offer",
        "what can you do",
    )
    urgency_signals = (
        "need gone",
        "moving",
        "this week",
        "today",
        "tonight",
        "asap",
        "must sell",
        "urgent",
    )
    bundle_signals = (
        "stand",
        "bundle",
        "accessory",
        "extras",
        "cover",
        "delivery",
        "table",
    )

    firmness = "mixed"
    if any(signal in combined for signal in firm_signals):
        firmness = "firm"
    elif any(signal in combined for signal in flexible_signals):
        firmness = "flexible"

    motivation = "high" if any(signal in combined for signal in urgency_signals) else "low"
    staleness = _estimate_listing_staleness(state.get("listing_posted_text", ""))

    reason = "not explicitly stated"
    reason_map = {
        "moving": "moving",
        "upgrade": "upgraded/replacing item",
        "new one": "upgraded/replacing item",
        "downsizing": "downsizing",
        "divorce": "personal-life change",
        "rent": "cash-flow pressure",
        "bills": "cash-flow pressure",
        "need gone": "wants fast pickup",
    }
    for needle, inferred in reason_map.items():
        if needle in combined:
            reason = inferred
            break

    if motivation == "high" or staleness == "stale":
        chosen_tactic = "deadline_cash_pickup"
    elif firmness == "firm" and state["negotiation_round"] >= 2:
        chosen_tactic = "walk_away"
    elif any(signal in combined for signal in bundle_signals):
        chosen_tactic = "bundle"
    elif firmness == "flexible":
        chosen_tactic = "anchor_low"
    else:
        chosen_tactic = "flinch"

    return {
        "firmness": firmness,
        "motivation": motivation,
        "staleness": staleness,
        "seller_reason": reason,
        "chosen_tactic": chosen_tactic,
        "tactic_rationale": (
            f"Fallback heuristic picked {chosen_tactic} based on seller tone and listing cues."
        ),
    }


async def _assess_seller_and_pick_tactic(
    state: WorkerState, existing_chat: list[AnyMessage]
) -> dict[str, str]:
    transcript_lines: list[str] = []
    for message in existing_chat[-12:]:
        role = "Buyer" if getattr(message, "type", "") == "ai" else "Seller"
        content = " ".join(str(message.content).split())
        transcript_lines.append(f"{role}: {content}")
    transcript = "\n".join(transcript_lines) or "No prior chat yet."

    prompt = SystemMessage(
        content=f"""Assess this seller, then choose the next negotiation tactic.

Seller + listing:
- Seller: {_target_seller_label(state)}
- Listed price: {state.get("listed_price")}
- Listing posted/date text: {state.get("listing_posted_text") or "unknown"}
- Listing description: {state.get("listing_description") or "none"}
- Stated reason for selling: {state.get("listing_reason") or "none visible"}
- Current offer floor: ${state['current_min_price']:.0f}
- Target budget: ${state['target_budget']:.0f}
- Round: {state['negotiation_round']}

Conversation so far:
{transcript}

Return ONLY valid JSON:
{{
  "firmness": "firm|mixed|flexible",
  "motivation": "high|medium|low",
  "staleness": "fresh|mid|stale|unknown",
  "seller_reason": "short text",
  "chosen_tactic": "anchor_low|bundle|flinch|walk_away|deadline_cash_pickup",
  "tactic_rationale": "one short sentence"
}}

Tactic selection rules:
- Use deadline_cash_pickup when urgency is high or listing feels stale.
- Use walk_away mostly when seller is firm after multiple rounds.
- Use bundle only when a plausible add-on exists.
- Use flinch when price feels high but seller may still engage.
- Use anchor_low when seller shows flexibility and can be moved."""
    )

    try:
        response = await model.ainvoke([prompt])
        parsed = _extract_json_object(str(response.content))
    except Exception:
        parsed = None

    if not parsed:
        return _fallback_tactic_assessment(state, existing_chat)

    normalized = {
        "firmness": str(parsed.get("firmness") or "mixed").lower(),
        "motivation": str(parsed.get("motivation") or "low").lower(),
        "staleness": str(parsed.get("staleness") or "unknown").lower(),
        "seller_reason": str(parsed.get("seller_reason") or "not explicitly stated"),
        "chosen_tactic": str(parsed.get("chosen_tactic") or "flinch").lower(),
        "tactic_rationale": str(parsed.get("tactic_rationale") or ""),
    }
    if normalized["chosen_tactic"] not in TACTIC_CHOICES:
        normalized["chosen_tactic"] = "flinch"
    return normalized


def _score_worker_priority(state: WorkerState) -> float:
    score = 0.0
    assessment = state.get("last_assessment") or {}
    motivation = str(assessment.get("motivation") or "").lower()
    staleness = str(assessment.get("staleness") or "").lower()
    firmness = str(assessment.get("firmness") or "").lower()
    last_reply = (state.get("last_reply") or "").lower()
    reason_blob = " ".join(
        [
            state.get("listing_reason", ""),
            state.get("listing_description", ""),
            state.get("listing_posted_text", ""),
            last_reply,
        ]
    ).lower()

    if motivation == "high":
        score += 3
    elif motivation == "medium":
        score += 1.5

    if staleness == "stale":
        score += 2
    elif staleness == "mid":
        score += 1

    if firmness == "flexible":
        score += 1.5
    elif firmness == "firm":
        score -= 0.5

    urgency_signals = ("moving", "need gone", "asap", "today", "tonight", "this week")
    if any(signal in reason_blob for signal in urgency_signals):
        score += 2

    listed_price = state.get("listed_price")
    if listed_price and listed_price > 0:
        discount = (listed_price - state["current_min_price"]) / listed_price
        score += max(0.0, min(discount, 0.4))

    return score


def _build_fallback_manager_plan(
    active_states: list[WorkerState],
) -> dict[str, Any]:
    if not active_states:
        return {"manager_brief": "No active workers.", "focus_worker": None, "directives": {}}

    anchor_worker = next(
        (
            state["worker_id"]
            for state in active_states
            if state.get("manager_role") == "aggressive_anchor"
        ),
        active_states[0]["worker_id"],
    )

    focus_worker = max(active_states, key=_score_worker_priority)["worker_id"]

    directives: dict[str, dict[str, str]] = {}
    for state in active_states:
        worker_id = state["worker_id"]
        role = "aggressive_anchor" if worker_id == anchor_worker else "closer"

        if worker_id == focus_worker:
            mode = "push_hard" if role == "aggressive_anchor" else "close_now"
            instruction = (
                "You are the focus listing this round. Push decisively for a near-term close."
            )
        elif role == "aggressive_anchor":
            mode = "probe_floor"
            instruction = (
                "Low-anchor to establish a team floor; avoid accidental close above the focus path."
            )
        else:
            mode = "stall"
            instruction = (
                "Keep seller warm and responsive, but do not progress toward close this round."
            )

        directives[worker_id] = {
            "role": role,
            "mode": mode,
            "instruction": instruction,
        }

    return {
        "manager_brief": (
            f"Focus {focus_worker}; anchor {anchor_worker} sets floor while non-focus "
            "closers stall."
        ),
        "focus_worker": focus_worker,
        "directives": directives,
    }


async def _manager_plan_round(states: list[WorkerState]) -> dict[str, Any]:
    active_states = [
        state
        for state in states
        if not state.get("success") and state["negotiation_round"] < state["max_rounds"]
    ]
    fallback = _build_fallback_manager_plan(active_states)
    if not active_states:
        return fallback

    summaries: list[str] = []
    for state in active_states:
        assessment = state.get("last_assessment") or {}
        summaries.append(
            " | ".join(
                [
                    f"worker={state['worker_id']}",
                    f"seller={state.get('seller_name')}",
                    f"role={state.get('manager_role') or 'unset'}",
                    f"round={state['negotiation_round']}",
                    f"current_offer={state['current_min_price']:.0f}",
                    f"listed_price={state.get('listed_price')}",
                    f"last_reply={state.get('last_reply') or 'none'}",
                    f"motivation={assessment.get('motivation', 'unknown')}",
                    f"firmness={assessment.get('firmness', 'unknown')}",
                    f"staleness={assessment.get('staleness', 'unknown')}",
                ]
            )
        )
    summary_blob = "\n".join(summaries)

    prompt = SystemMessage(
        content=f"""You are the Dealbot manager. Coordinate workers strategically.

Your job each round:
1) Assign exactly one aggressive_anchor worker.
2) Pick one focus_worker most likely to close best now (motivated or stale seller).
3) Tell focus worker to push harder/close now.
4) Tell non-focus closers to stall so we do not accidentally close a worse deal first.
5) Keep anchor worker probing a low floor unless they are also focus.
6) Use mode pull_off only when a worker should be retired and its browser closed.

Active workers snapshot:
{summary_blob}

Return ONLY valid JSON:
{{
  "manager_brief": "one concise sentence",
  "focus_worker": "<worker_id>",
  "directives": {{
    "<worker_id>": {{
      "role": "aggressive_anchor|closer",
      "mode": "probe_floor|push_hard|close_now|stall|pull_off",
      "instruction": "one concise instruction"
    }}
  }}
}}
"""
    )

    parsed: dict[str, Any] | None = None
    try:
        response = await model.ainvoke([prompt])
        parsed = _extract_json_object(str(response.content))
    except Exception:
        parsed = None

    if not parsed:
        return fallback

    directives_raw = parsed.get("directives")
    focus_worker = str(parsed.get("focus_worker") or fallback["focus_worker"])
    manager_brief = str(parsed.get("manager_brief") or fallback["manager_brief"])
    if not isinstance(directives_raw, dict):
        return fallback

    active_ids = {state["worker_id"] for state in active_states}
    directives: dict[str, dict[str, str]] = {}
    for worker_id in active_ids:
        raw = directives_raw.get(worker_id)
        if not isinstance(raw, dict):
            raw = fallback["directives"].get(worker_id, {})
        role = str(raw.get("role") or "closer").lower()
        mode = str(raw.get("mode") or "close_now").lower()
        instruction = str(raw.get("instruction") or "")
        if role not in MANAGER_ROLE_CHOICES:
            role = "closer"
        if mode not in MANAGER_MODE_CHOICES:
            mode = "close_now"
        directives[worker_id] = {
            "role": role,
            "mode": mode,
            "instruction": instruction
            or "Negotiate naturally while following manager strategy.",
        }

    anchor_ids = [
        worker_id
        for worker_id, item in directives.items()
        if item["role"] == "aggressive_anchor"
    ]
    if len(anchor_ids) != 1:
        fallback_anchor = next(
            (
                worker_id
                for worker_id, item in fallback.get("directives", {}).items()
                if item.get("role") == "aggressive_anchor" and worker_id in active_ids
            ),
            None,
        )
        anchor_worker = fallback_anchor or next(iter(active_ids))
        for worker_id in directives:
            directives[worker_id]["role"] = (
                "aggressive_anchor" if worker_id == anchor_worker else "closer"
            )

    if focus_worker not in active_ids:
        focus_worker = fallback["focus_worker"]

    return {
        "manager_brief": manager_brief,
        "focus_worker": focus_worker,
        "directives": directives,
    }


async def create_and_send(
    state: WorkerState,
    *,
    event_publisher: AgentEventPublisher | None = None,
) -> dict[str, Any]:
    worker_id = state["worker_id"]
    existing_chat = memory.to_langchain_messages(worker_id)
    _emit_event(
        event_publisher,
        event_type="worker_thinking",
        actor_type="worker",
        actor_id=worker_id,
        summary=f"{worker_id} is reasoning about the next message",
        details={
            "phase": "negotiation_turn",
            "manager_mode": state.get("manager_mode"),
            "manager_instruction": state.get("manager_instruction"),
            "last_reply": state.get("last_reply"),
        },
    )
    assessment = await _assess_seller_and_pick_tactic(state, existing_chat)
    manager_role = state.get("manager_role", "closer")
    manager_mode = state.get("manager_mode", "close_now")
    manager_instruction = state.get("manager_instruction", "")
    chosen_tactic = assessment["chosen_tactic"]
    if manager_mode == "probe_floor":
        chosen_tactic = "anchor_low"
    elif manager_mode == "push_hard" and chosen_tactic not in {
        "deadline_cash_pickup",
        "walk_away",
    }:
        chosen_tactic = "deadline_cash_pickup"
    tactic_instruction = TACTIC_PLAYBOOK.get(chosen_tactic, TACTIC_PLAYBOOK["flinch"])
    comparable = await asyncio.to_thread(
        memory.find_best_comparable_listing,
        worker_id=worker_id,
        listing_url=state["listing_url"],
        title=state.get("listing_title", ""),
        description=state.get("listing_description", ""),
        condition_text=(
            f"{state.get('listing_reason', '')} {state.get('listing_posted_text', '')}".strip()
        ),
    )
    if comparable:
        comp_price = float(comparable["price"])
        comp_worker = comparable.get("worker_id")
        comp_similarity = comparable.get("similarity")
        if comp_similarity is not None:
            leverage = (
                f"A teammate ({comp_worker}) negotiated a closely comparable listing "
                f"(similarity {float(comp_similarity):.2f}) at ${comp_price:.0f}. "
                "Use this as condition-aware leverage. Do not name the teammate or listing."
            )
        else:
            leverage = (
                f"A teammate ({comp_worker}) got a lower team price at ${comp_price:.0f}. "
                "Use this as a weak fallback leverage signal only when condition seems comparable."
            )
    else:
        leverage = "No peer worker has a trustworthy comparable price yet."

    system_instruction = SystemMessage(
        content=f"""You are a professional but friendly Facebook Marketplace buyer negotiating for a lower price.
You are messaging {_target_seller_label(state)}.

Rules:
- Keep messages to 1-2 short sentences.
- Your current offer floor is ${state['current_min_price']:.0f}; do not offer below that.
- Never exceed ${state['max_price']:.0f}.
- Goal budget is ${state['target_budget']:.0f}.
- Manager role: {manager_role}
- Manager mode this round: {manager_mode}
- Manager instruction: {manager_instruction}
- Seller read:
  - Firmness: {assessment['firmness']}
  - Motivation to sell soon: {assessment['motivation']}
  - Listing staleness: {assessment['staleness']}
  - Seller reason signal: {assessment['seller_reason']}
  - Chosen tactic: {chosen_tactic}
  - Tactic rationale: {assessment['tactic_rationale']}
- Apply this tactic guidance now: {tactic_instruction}
- If tactic is deadline_cash_pickup, include a concrete immediate pickup time window.
- If manager mode is probe_floor, prioritize testing a lower anchor over closing.
- If manager mode is close_now, prioritize a realistic close with immediate pickup.
- {leverage}
- Write ONLY the message text to send to the seller. No quotes or explanation."""
    )
    print(
        f"[{worker_id}] formulating message | mode={manager_mode} "
        f"tactic={chosen_tactic} | plan={manager_instruction or 'none'}",
        flush=True,
    )

    ai_response = await model.ainvoke([system_instruction, *existing_chat])
    message_content = str(ai_response.content).strip()
    _emit_event(
        event_publisher,
        event_type="message_sent",
        actor_type="worker",
        actor_id=worker_id,
        summary=f"{worker_id} sent a negotiation message",
        details={
            "to": "seller",
            "message": message_content,
            "manager_mode": manager_mode,
            "tactic": chosen_tactic,
        },
    )

    chat = await send_listing_message(
        state["stagehand_context_id"],
        message_content,
        listing_url=state["listing_url"],
        reply_wait_ms=REPLY_WAIT_MS,
    )
    reply = chat.get("reply_message")
    if reply:
        _emit_event(
            event_publisher,
            event_type="message_received",
            actor_type="worker",
            actor_id=worker_id,
            summary=f"{worker_id} received a seller reply",
            details={"from": "seller", "message": reply},
        )

    await asyncio.to_thread(
        memory.record_exchange,
        worker_id,
        sent_message=message_content,
        reply_message=reply,
        current_min_price=state["current_min_price"],
        status="negotiating",
    )
    await asyncio.to_thread(
        memory.update_short_term,
        worker_id,
        last_tactic=chosen_tactic,
        last_assessment=assessment,
        manager_role=manager_role,
        manager_mode=manager_mode,
        manager_instruction=manager_instruction,
        manager_focus_worker=state.get("manager_focus_worker"),
        manager_round_brief=state.get("manager_round_brief"),
        awaiting_reply=True,
    )
    print(
        f"[{worker_id}] saved context to redis; awaiting seller reply.",
        flush=True,
    )

    return {
        "messages": [AIMessage(content=message_content)],
        "last_sent": message_content,
        "last_reply": reply,
        "last_tactic": chosen_tactic,
        "last_assessment": assessment,
        "negotiation_round": state["negotiation_round"] + 1,
        "awaiting_reply": True,
        "needs_manager_review": False,
    }


async def send_stall_message(
    state: WorkerState,
    *,
    event_publisher: AgentEventPublisher | None = None,
) -> dict[str, Any]:
    worker_id = state["worker_id"]
    existing_chat = memory.to_langchain_messages(worker_id)
    print(
        f"[{worker_id}] formulating message | mode=stall "
        f"| plan={state.get('manager_instruction') or 'none'}",
        flush=True,
    )
    prompt = SystemMessage(
        content=f"""You are keeping a Facebook Marketplace seller warm while your team prioritizes another listing.
You are messaging {_target_seller_label(state)}.

Rules:
- Send exactly 1 short sentence.
- Be polite and human.
- Do NOT make a new offer or accept any deal.
- Do NOT mention having multiple agents/listings.
- Lightly delay: say you are checking timing and will confirm soon.
- Output ONLY the message text."""
    )
    ai_response = await model.ainvoke([prompt, *existing_chat])
    message_content = str(ai_response.content).strip()
    _emit_event(
        event_publisher,
        event_type="message_sent",
        actor_type="worker",
        actor_id=worker_id,
        summary=f"{worker_id} sent a stall message",
        details={
            "to": "seller",
            "message": message_content,
            "manager_mode": "stall",
            "phase": "stall",
        },
    )

    chat = await send_listing_message(
        state["stagehand_context_id"],
        message_content,
        listing_url=state["listing_url"],
        reply_wait_ms=REPLY_WAIT_MS,
    )
    reply = chat.get("reply_message")
    if reply:
        _emit_event(
            event_publisher,
            event_type="message_received",
            actor_type="worker",
            actor_id=worker_id,
            summary=f"{worker_id} received seller response while stalling",
            details={"from": "seller", "message": reply, "phase": "stall"},
        )

    await asyncio.to_thread(
        memory.record_exchange,
        worker_id,
        sent_message=message_content,
        reply_message=reply,
        current_min_price=state["current_min_price"],
        status="negotiating",
    )
    await asyncio.to_thread(
        memory.update_short_term,
        worker_id,
        manager_role=state.get("manager_role", "closer"),
        manager_mode="stall",
        manager_instruction=state.get("manager_instruction", ""),
        manager_focus_worker=state.get("manager_focus_worker"),
        manager_round_brief=state.get("manager_round_brief"),
        awaiting_reply=True,
    )
    print(
        f"[{worker_id}] saved context to redis; awaiting seller reply.",
        flush=True,
    )

    return {
        "messages": [AIMessage(content=message_content)],
        "last_sent": message_content,
        "last_reply": reply,
        "negotiation_round": state["negotiation_round"] + 1,
        "awaiting_reply": True,
        "needs_manager_review": False,
    }


async def pull_off_worker(
    state: WorkerState,
    *,
    event_publisher: AgentEventPublisher | None = None,
) -> dict[str, Any]:
    worker_id = state["worker_id"]
    closed = await close_active_listing_browser(state["stagehand_context_id"])
    await asyncio.to_thread(
        memory.update_short_term,
        worker_id,
        status="pulled_off",
        manager_role=state.get("manager_role", "closer"),
        manager_mode="pull_off",
        manager_instruction=state.get("manager_instruction", ""),
        manager_focus_worker=state.get("manager_focus_worker"),
        manager_round_brief=state.get("manager_round_brief"),
    )
    print(
        f"[manager] pulled off {worker_id}; browser {'closed' if closed else 'not found'}",
        flush=True,
    )
    _emit_event(
        event_publisher,
        event_type="worker_killed",
        actor_type="manager",
        actor_id="manager",
        summary=f"Manager retired {worker_id}",
        details={
            "worker_id": worker_id,
            "browser_closed": closed,
            "reason": state.get("manager_instruction") or "manager pull_off directive",
        },
    )
    return {
        "messages": [],
        "last_sent": state.get("last_sent"),
        "last_reply": state.get("last_reply"),
        "negotiation_round": state["max_rounds"],
        "success": False,
        "awaiting_reply": False,
        "needs_manager_review": False,
    }


async def evaluate_response(
    state: WorkerState,
    *,
    event_publisher: AgentEventPublisher | None = None,
) -> dict[str, Any]:
    worker_id = state["worker_id"]
    reply = (state.get("last_reply") or "").strip()
    existing_chat = memory.to_langchain_messages(worker_id)

    if not reply:
        return {
            "seller_budged": False,
            "success": False,
            "messages": [],
            "awaiting_reply": True,
        }
    _emit_event(
        event_publisher,
        event_type="message_received",
        actor_type="worker",
        actor_id=worker_id,
        summary=f"{worker_id} is evaluating a seller reply",
        details={"from": "seller", "message": reply, "phase": "evaluation"},
    )

    if _success_from_reply(reply, state["target_budget"]):
        await asyncio.to_thread(
            memory.update_short_term,
            worker_id,
            status="success",
        )
        price = parse_price(reply)
        if price is not None:
            await asyncio.to_thread(
                memory.set_global_lowest_price,
                price,
                worker_id=worker_id,
                listing_url=state["listing_url"],
            )
        _emit_event(
            event_publisher,
            event_type="worker_success",
            actor_type="worker",
            actor_id=worker_id,
            summary=f"{worker_id} reached target budget agreement",
            details={
                "agreed_price": parse_price(reply),
                "target_budget": state["target_budget"],
            },
        )
        return {
            "seller_budged": False,
            "success": True,
            "messages": [HumanMessage(content=reply)],
            "awaiting_reply": False,
        }

    system_instruction = SystemMessage(
        content=f"""You are evaluating a seller's reply during price negotiation.
Seller: {_target_seller_label(state)}
Seller reply: "{reply}"
Your current offer floor: ${state['current_min_price']:.0f}
Target budget: ${state['target_budget']:.0f}

Based on the reply and chat context, is the seller still willing to negotiate on price?
Answer ONLY "Yes" or "No"."""
    )

    ai_response = await model.ainvoke([system_instruction, *existing_chat])
    answer = str(ai_response.content).strip().lower()
    budged = answer.startswith("y")
    _emit_event(
        event_publisher,
        event_type="worker_thinking",
        actor_type="worker",
        actor_id=worker_id,
        summary=f"{worker_id} evaluated seller flexibility",
        details={
            "phase": "reply_evaluation",
            "seller_budged": budged,
            "analysis_answer": answer,
        },
    )

    return {
        "seller_budged": budged,
        "success": False,
        "messages": [HumanMessage(content=reply)],
        "awaiting_reply": False,
    }


async def update_memory(
    state: WorkerState,
    *,
    event_publisher: AgentEventPublisher | None = None,
) -> dict[str, Any]:
    worker_id = state["worker_id"]
    new_price = max(state["target_budget"], state["current_min_price"] * 0.9)

    await asyncio.to_thread(
        memory.update_short_term,
        worker_id,
        current_min_price=new_price,
    )

    listed = state.get("listed_price")
    if listed is not None and new_price < listed:
        await asyncio.to_thread(
            memory.set_global_lowest_price,
            new_price,
            worker_id=worker_id,
            listing_url=state["listing_url"],
        )
    _emit_event(
        event_publisher,
        event_type="worker_price_update",
        actor_type="worker",
        actor_id=worker_id,
        summary=f"{worker_id} updated negotiation floor",
        details={
            "new_floor_price": new_price,
            "target_budget": state["target_budget"],
            "listed_price": listed,
        },
    )

    return {"current_min_price": new_price}


async def wrap_up(
    state: WorkerState,
    *,
    event_publisher: AgentEventPublisher | None = None,
) -> dict[str, Any]:
    worker_id = state["worker_id"]
    existing_chat = memory.to_langchain_messages(worker_id)
    assessment = await _assess_seller_and_pick_tactic(state, existing_chat)
    chosen_tactic = assessment["chosen_tactic"]

    if chosen_tactic == "deadline_cash_pickup":
        final_message = (
            f"I can do ${state['current_min_price']:.0f} cash and pick up tonight "
            "if that works for you."
        )
    elif chosen_tactic == "walk_away":
        final_message = (
            "No worries at all - I found another option nearby, but if you can do "
            f"${state['current_min_price']:.0f} today I can still pick it up."
        )
    else:
        final_message = "Thanks for your time!"

    await send_listing_message(
        state["stagehand_context_id"],
        final_message,
        listing_url=state["listing_url"],
        reply_wait_ms=1000,
    )
    _emit_event(
        event_publisher,
        event_type="message_sent",
        actor_type="worker",
        actor_id=worker_id,
        summary=f"{worker_id} sent final wrap-up message",
        details={
            "to": "seller",
            "message": final_message,
            "phase": "wrap_up",
            "tactic": chosen_tactic,
        },
    )
    await asyncio.to_thread(
        memory.record_exchange,
        worker_id,
        sent_message=final_message,
        reply_message=None,
        status="stopped",
    )
    await asyncio.to_thread(
        memory.update_short_term,
        worker_id,
        last_tactic=chosen_tactic,
        last_assessment=assessment,
        manager_role=state.get("manager_role", "closer"),
        manager_mode=state.get("manager_mode", "close_now"),
        manager_instruction=state.get("manager_instruction", ""),
        manager_focus_worker=state.get("manager_focus_worker"),
        manager_round_brief=state.get("manager_round_brief"),
        awaiting_reply=True,
    )
    return {
        "messages": [AIMessage(content=final_message)],
        "seller_budged": False,
        "success": False,
        "last_tactic": chosen_tactic,
        "last_assessment": assessment,
        "awaiting_reply": True,
    }


def build_worker_input(assignment: dict[str, Any]) -> WorkerState:
    worker_id = assignment["worker_name"]
    long_term = memory.get_long_term(worker_id)
    short_term = memory.get_short_term(worker_id)
    product = long_term.get("product") or assignment.get("product") or {}
    job = long_term.get("job") or {}

    context_id = assignment.get("stagehand_context_id") or assignment.get(
        "browserbase_session_id"
    )
    if not context_id:
        raise ValueError(f"Missing stagehand context id for worker {worker_id}")

    return {
        "worker_id": worker_id,
        "stagehand_context_id": context_id,
        "seller_name": product.get("seller_name") or "seller",
        "listing_title": product.get("title") or "item",
        "listing_description": product.get("description") or "",
        "listing_posted_text": (
            product.get("date_listed")
            or product.get("listed_at")
            or product.get("date_posted")
            or ""
        ),
        "listing_reason": product.get("reason_for_selling") or "",
        "listing_url": assignment.get("listing_url") or long_term.get("listing_url", ""),
        "listed_price": parse_price(product.get("price")),
        "current_min_price": float(
            short_term.get("current_min_price")
            or assignment.get("current_min_price")
            or job.get("target_budget", 40)
        ),
        "target_budget": float(job.get("target_budget", 40)),
        "max_price": float(job.get("max_price", 50)),
        "messages": memory.to_langchain_messages(worker_id),
        "seller_budged": False,
        "negotiation_round": 0,
        "max_rounds": MAX_NEGOTIATION_ROUNDS,
        "success": False,
        "last_sent": short_term.get("last_sent_message") or assignment.get("first_message"),
        "last_reply": short_term.get("last_reply_message") or assignment.get("opener_reply"),
        "last_tactic": short_term.get("last_tactic"),
        "last_assessment": short_term.get("last_assessment"),
        "manager_role": short_term.get("manager_role") or "closer",
        "manager_mode": short_term.get("manager_mode") or "close_now",
        "manager_instruction": short_term.get("manager_instruction") or "",
        "manager_focus_worker": short_term.get("manager_focus_worker"),
        "manager_round_brief": short_term.get("manager_round_brief"),
        "awaiting_reply": not bool(
            (short_term.get("last_reply_message") or assignment.get("opener_reply") or "").strip()
        ),
        "needs_manager_review": False,
    }


async def _apply_manager_plan_to_states(
    states: list[WorkerState], manager_plan: dict[str, Any]
) -> None:
    directives = manager_plan.get("directives") or {}
    focus_worker = manager_plan.get("focus_worker")
    manager_brief = str(manager_plan.get("manager_brief") or "")

    for state in states:
        if state.get("success") or state["negotiation_round"] >= state["max_rounds"]:
            continue
        directive = directives.get(state["worker_id"])
        if not isinstance(directive, dict):
            continue

        role = str(directive.get("role") or "closer").lower()
        mode = str(directive.get("mode") or "close_now").lower()
        instruction = str(directive.get("instruction") or "")
        if role not in MANAGER_ROLE_CHOICES:
            role = "closer"
        if mode not in MANAGER_MODE_CHOICES:
            mode = "close_now"

        state["manager_role"] = role
        state["manager_mode"] = mode
        state["manager_instruction"] = instruction
        state["manager_focus_worker"] = focus_worker
        state["manager_round_brief"] = manager_brief
        state["needs_manager_review"] = False

        await asyncio.to_thread(
            memory.update_short_term,
            state["worker_id"],
            manager_role=role,
            manager_mode=mode,
            manager_instruction=instruction,
            manager_focus_worker=focus_worker,
            manager_round_brief=manager_brief,
            status="negotiating",
            needs_manager_review=False,
        )


async def _listen_for_live_coordination_events(stop_event: asyncio.Event) -> None:
    pubsub = await asyncio.to_thread(memory.open_live_event_subscription)
    try:
        while not stop_event.is_set():
            event = await asyncio.to_thread(memory.read_live_event, pubsub, 0.2)
            if not event:
                await asyncio.sleep(0.05)
                continue
            event_type = str(event.get("event_type") or "")
            if event_type == "price_improved":
                print(
                    "[live-bus] "
                    f"{event.get('worker_id')} improved to ${float(event.get('new_price', 0)):.0f}",
                    flush=True,
                )
    finally:
        await asyncio.to_thread(pubsub.close)


async def _poll_late_seller_replies(
    states: list[WorkerState],
    stop_event: asyncio.Event,
    *,
    event_publisher: AgentEventPublisher | None = None,
) -> None:
    poll_every_s = max(0.5, REPLY_POLL_INTERVAL_MS / 1000.0)
    while not stop_event.is_set():
        poll_targets = [
            state
            for state in states
            if not state.get("success")
            and state["negotiation_round"] < state["max_rounds"]
            and state.get("manager_mode") != "pull_off"
            and bool((state.get("last_sent") or "").strip())
        ]

        async def _poll_state(state: WorkerState) -> None:
            sent_message = str(state.get("last_sent") or "").strip()
            if not sent_message:
                return
            worker_id = state["worker_id"]
            try:
                reply = await poll_listing_reply(
                    state["stagehand_context_id"],
                    sent_message=sent_message,
                    listing_url=state.get("listing_url"),
                )
            except Exception as exc:
                print(f"[reply-poll] {worker_id} poll failed: {exc}", flush=True)
                return
            if not reply:
                return

            current_reply = str(state.get("last_reply") or "").strip()
            if reply == current_reply:
                return

            short_term = await asyncio.to_thread(memory.get_short_term, worker_id)
            known_reply = str(short_term.get("last_reply_message") or "").strip()
            if reply == known_reply:
                state["last_reply"] = reply
                state["awaiting_reply"] = False
                return

            state["last_reply"] = reply
            state["awaiting_reply"] = False
            state["needs_manager_review"] = True
            _emit_event(
                event_publisher,
                event_type="message_received",
                actor_type="worker",
                actor_id=worker_id,
                summary=f"{worker_id} received a late seller reply",
                details={"from": "seller", "message": reply, "phase": "late_poll"},
            )
            await asyncio.to_thread(
                memory.append_chat_messages,
                worker_id,
                [{"role": "user", "content": reply}],
            )
            await asyncio.to_thread(
                memory.update_short_term,
                worker_id,
                last_reply_message=reply,
                status="negotiating",
                awaiting_reply=False,
                needs_manager_review=True,
            )
            print(
                f"[reply-poll] {worker_id} saved late reply to redis; "
                "manager directive check requested.",
                flush=True,
            )

        if poll_targets:
            await asyncio.gather(*(_poll_state(state) for state in poll_targets))

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_every_s)
        except asyncio.TimeoutError:
            pass


async def launch_negotiation(
    assignments: list[dict[str, Any]],
    *,
    event_publisher: AgentEventPublisher | None = None,
) -> list[dict[str, Any]]:
    states = [build_worker_input(item) for item in assignments]
    if not states:
        return states
    _emit_event(
        event_publisher,
        event_type="negotiation_started",
        actor_type="manager",
        actor_id="manager",
        summary=f"Manager launched negotiation rounds for {len(states)} workers",
        details={"worker_ids": [state["worker_id"] for state in states]},
    )

    print(
        f"[timing] reply_wait_ms={REPLY_WAIT_MS} "
        f"reply_poll_interval_ms={REPLY_POLL_INTERVAL_MS}",
        flush=True,
    )

    stop_live_listener = asyncio.Event()
    stop_reply_poller = asyncio.Event()
    live_listener_task = asyncio.create_task(
        _listen_for_live_coordination_events(stop_live_listener)
    )
    reply_poller_task = asyncio.create_task(
        _poll_late_seller_replies(
            states,
            stop_reply_poller,
            event_publisher=event_publisher,
        )
    )

    # Bootstrap manager roles once before round execution.
    bootstrap_plan = _build_fallback_manager_plan(
        [state for state in states if not state.get("success")]
    )
    try:
        await _apply_manager_plan_to_states(states, bootstrap_plan)
        _emit_event(
            event_publisher,
            event_type="manager_thinking",
            actor_type="manager",
            actor_id="manager",
            summary="Manager set initial worker strategy",
            details={
                "focus_worker": bootstrap_plan.get("focus_worker"),
                "manager_brief": bootstrap_plan.get("manager_brief"),
                "directives": bootstrap_plan.get("directives"),
            },
        )
        for worker_id, directive in (bootstrap_plan.get("directives") or {}).items():
            if not isinstance(directive, dict):
                continue
            _emit_event(
                event_publisher,
                event_type="manager_directive",
                actor_type="manager",
                actor_id="manager",
                summary=f"Manager assigned directive to {worker_id}",
                details={
                    "worker_id": worker_id,
                    "role": directive.get("role"),
                    "mode": directive.get("mode"),
                    "instruction": directive.get("instruction"),
                    "focus_worker": bootstrap_plan.get("focus_worker"),
                },
            )

        while True:
            active_states = [
                state
                for state in states
                if not state.get("success") and state["negotiation_round"] < state["max_rounds"]
            ]
            if not active_states:
                break

            review_workers = [
                state["worker_id"] for state in active_states if state.get("needs_manager_review")
            ]
            if review_workers:
                print(
                    "[manager] checking refreshed directives for: "
                    + ", ".join(review_workers),
                    flush=True,
                )

            manager_plan = await _manager_plan_round(states)
            focus_worker = manager_plan.get("focus_worker")
            manager_brief = manager_plan.get("manager_brief")
            _emit_event(
                event_publisher,
                event_type="manager_thinking",
                actor_type="manager",
                actor_id="manager",
                summary=f"Manager planned round focus on {focus_worker}",
                details={
                    "focus_worker": focus_worker,
                    "manager_brief": manager_brief,
                    "review_workers": review_workers,
                    "directives": manager_plan.get("directives"),
                },
            )
            print(
                f"[manager] focus={focus_worker} | {manager_brief}",
                flush=True,
            )
            for worker_id, directive in (manager_plan.get("directives") or {}).items():
                if not isinstance(directive, dict):
                    continue
                _emit_event(
                    event_publisher,
                    event_type="manager_directive",
                    actor_type="manager",
                    actor_id="manager",
                    summary=f"Manager directed {worker_id} ({directive.get('mode')})",
                    details={
                        "worker_id": worker_id,
                        "role": directive.get("role"),
                        "mode": directive.get("mode"),
                        "instruction": directive.get("instruction"),
                        "focus_worker": focus_worker,
                    },
                )
                print(
                    f"[manager] {worker_id}: role={directive.get('role')} "
                    f"mode={directive.get('mode')} | {directive.get('instruction')}",
                    flush=True,
                )
            await _apply_manager_plan_to_states(states, manager_plan)

            async def _run_worker_round(state: WorkerState) -> None:
                if state.get("manager_mode") == "pull_off":
                    pull_result = await pull_off_worker(
                        state,
                        event_publisher=event_publisher,
                    )
                    state.update(pull_result)
                    return

                evaluation = await evaluate_response(
                    state,
                    event_publisher=event_publisher,
                )
                state.update(evaluation)

                if state.get("success"):
                    return

                if state.get("awaiting_reply") and not (state.get("last_reply") or "").strip():
                    print(
                        f"[{state['worker_id']}] awaiting seller reply; "
                        "no new directive action this tick.",
                        flush=True,
                    )
                    return

                if state.get("seller_budged"):
                    price_update = await update_memory(
                        state,
                        event_publisher=event_publisher,
                    )
                    state.update(price_update)

                if state["negotiation_round"] >= state["max_rounds"]:
                    wrap_result = await wrap_up(
                        state,
                        event_publisher=event_publisher,
                    )
                    state.update(wrap_result)
                    return

                if state.get("manager_mode") == "stall":
                    turn_result = await send_stall_message(
                        state,
                        event_publisher=event_publisher,
                    )
                else:
                    turn_result = await create_and_send(
                        state,
                        event_publisher=event_publisher,
                    )
                state.update(turn_result)

                if (
                    not state.get("success")
                    and state["negotiation_round"] >= state["max_rounds"]
                ):
                    wrap_result = await wrap_up(
                        state,
                        event_publisher=event_publisher,
                    )
                    state.update(wrap_result)

            await asyncio.gather(*(_run_worker_round(state) for state in active_states))
    finally:
        stop_live_listener.set()
        stop_reply_poller.set()
        try:
            await asyncio.wait_for(live_listener_task, timeout=2.0)
        except asyncio.TimeoutError:
            live_listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await live_listener_task
        try:
            await asyncio.wait_for(reply_poller_task, timeout=2.0)
        except asyncio.TimeoutError:
            reply_poller_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reply_poller_task

    _emit_event(
        event_publisher,
        event_type="negotiation_completed",
        actor_type="manager",
        actor_id="manager",
        summary="Manager completed negotiation pipeline",
        details={
            "workers": [
                {
                    "worker_id": state["worker_id"],
                    "success": state.get("success", False),
                    "rounds_used": state.get("negotiation_round"),
                    "last_mode": state.get("manager_mode"),
                }
                for state in states
            ]
        },
    )

    return states


if __name__ == "__main__":
    raise SystemExit(
        "Run the full pipeline with: python agent_initializer.py"
    )
