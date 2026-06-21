"""
LangGraph negotiation loop for Dealbot workers.

Flow per worker (after agent_initializer sends the opening message):
  evaluate opener/reply -> if seller may budge, lower floor and send counter-offer
  -> evaluate again -> repeat until success, max rounds, or wrap up.
"""

from __future__ import annotations

import asyncio
import operator
import os
from typing import Annotated, Any, Literal, TypedDict

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from browserb import send_listing_message
from memory import AgentMemoryStore, parse_price


load_dotenv()

model = init_chat_model("claude-sonnet-4-6", temperature=0.0)
memory = AgentMemoryStore()

MAX_NEGOTIATION_ROUNDS = 8
REPLY_WAIT_MS = int(os.environ.get("REPLY_WAIT_MS", "4000"))


class WorkerState(TypedDict):
    worker_id: str
    browserbase_session_id: str
    seller_name: str
    listing_title: str
    listing_url: str
    listed_price: float | None
    current_min_price: float
    target_budget: float
    max_price: float
    messages: Annotated[list[AnyMessage], operator.add]
    seller_budged: bool
    negotiation_round: int
    max_rounds: int
    success: bool
    last_sent: str | None
    last_reply: str | None


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


async def create_and_send(state: WorkerState) -> dict[str, Any]:
    worker_id = state["worker_id"]
    existing_chat = memory.to_langchain_messages(worker_id)
    floor = memory.get_market_floor()
    global_lowest = floor.get("lowest_price_seen")
    peer_worker = floor.get("winning_worker_id")
    leverage = (
        f"Another worker agent ({peer_worker}) already got this item for "
        f"${float(global_lowest):.0f}. Use that team price as leverage to push "
        f"this seller lower. Do not name the other seller or worker."
        if global_lowest is not None and peer_worker != worker_id
        else "No peer worker has a better team price yet."
    )

    system_instruction = SystemMessage(
        content=f"""You are a professional but friendly Facebook Marketplace buyer negotiating for a lower price.
You are messaging {_target_seller_label(state)}.

Rules:
- Keep messages to 1-2 short sentences.
- Your current offer floor is ${state['current_min_price']:.0f}; do not offer below that.
- Never exceed ${state['max_price']:.0f}.
- Goal budget is ${state['target_budget']:.0f}.
- {leverage}
- Write ONLY the message text to send to the seller. No quotes or explanation."""
    )

    ai_response = await model.ainvoke([system_instruction, *existing_chat])
    message_content = str(ai_response.content).strip()

    chat = await send_listing_message(
        state["browserbase_session_id"],
        message_content,
        reply_wait_ms=REPLY_WAIT_MS,
    )
    reply = chat.get("reply_message")

    await asyncio.to_thread(
        memory.record_exchange,
        worker_id,
        sent_message=message_content,
        reply_message=reply,
        current_min_price=state["current_min_price"],
        status="negotiating",
    )

    return {
        "messages": [AIMessage(content=message_content)],
        "last_sent": message_content,
        "last_reply": reply,
        "negotiation_round": state["negotiation_round"] + 1,
    }


async def evaluate_response(state: WorkerState) -> dict[str, Any]:
    worker_id = state["worker_id"]
    reply = (state.get("last_reply") or "").strip()
    existing_chat = memory.to_langchain_messages(worker_id)

    if not reply:
        return {
            "seller_budged": False,
            "success": False,
            "messages": [],
        }

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
        return {
            "seller_budged": False,
            "success": True,
            "messages": [HumanMessage(content=reply)],
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

    return {
        "seller_budged": budged,
        "success": False,
        "messages": [HumanMessage(content=reply)],
    }


def route_after_evaluate(state: WorkerState) -> Literal["update_memory", "wrap_up", "done"]:
    if state.get("success"):
        return "done"
    if state.get("seller_budged") and state["negotiation_round"] < state["max_rounds"]:
        return "update_memory"
    return "wrap_up"


async def update_memory(state: WorkerState) -> dict[str, Any]:
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

    return {"current_min_price": new_price}


async def wrap_up(state: WorkerState) -> dict[str, Any]:
    final_message = "Thanks for your time!"
    await send_listing_message(
        state["browserbase_session_id"],
        final_message,
        reply_wait_ms=1000,
    )
    await asyncio.to_thread(
        memory.record_exchange,
        state["worker_id"],
        sent_message=final_message,
        reply_message=None,
        status="stopped",
    )
    return {
        "messages": [AIMessage(content=final_message)],
        "seller_budged": False,
        "success": False,
    }


builder = StateGraph(WorkerState)
builder.add_node("create_and_send", create_and_send)
builder.add_node("evaluate", evaluate_response)
builder.add_node("update_memory", update_memory)
builder.add_node("wrap_up", wrap_up)

builder.add_edge(START, "evaluate")
builder.add_conditional_edges(
    "evaluate",
    route_after_evaluate,
    {
        "update_memory": "update_memory",
        "wrap_up": "wrap_up",
        "done": END,
    },
)
builder.add_edge("update_memory", "create_and_send")
builder.add_edge("create_and_send", "evaluate")
builder.add_edge("wrap_up", END)

negotiation_agent = builder.compile()


def build_worker_input(assignment: dict[str, Any]) -> WorkerState:
    worker_id = assignment["worker_name"]
    long_term = memory.get_long_term(worker_id)
    short_term = memory.get_short_term(worker_id)
    product = long_term.get("product") or assignment.get("product") or {}
    job = long_term.get("job") or {}

    return {
        "worker_id": worker_id,
        "browserbase_session_id": assignment["browserbase_session_id"],
        "seller_name": product.get("seller_name") or "seller",
        "listing_title": product.get("title") or "item",
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
    }


async def launch_negotiation(assignments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    inputs = [build_worker_input(item) for item in assignments]
    results = await negotiation_agent.abatch(inputs)
    return results


if __name__ == "__main__":
    raise SystemExit(
        "Run the full pipeline with: python agent_initializer.py"
    )
