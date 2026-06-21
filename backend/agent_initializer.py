"""
Assign worker agents to Marketplace listings, persist memory in Redis,
scrape listing data, send opening seller messages, then run negotiation loops.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import redis
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.messages import SystemMessage

from agent_events import AgentEventPublisher
from browserb import initialize_browsers, send_listing_message
from memory import AgentMemoryStore, parse_price

model = init_chat_model("claude-sonnet-4-6", temperature=0.4)

DEFAULT_PRODUCT_NAME = "Gray Couch"
DEFAULT_TARGET_BUDGET = 40
DEFAULT_MAX_PRICE = 50
DEFAULT_LISTING_URLS = [
    "https://www.facebook.com/marketplace/item/985732227779817/",
    "https://www.facebook.com/marketplace/item/1346267354014284/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
    "https://www.facebook.com/marketplace/item/954225447099368/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
]


def _log(message: str) -> None:
    print(message, flush=True)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.split("#", 1)[0].strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _first_name(full_name: str | None) -> str:
    if not full_name:
        return ""
    cleaned = " ".join(str(full_name).replace(",", " ").split()).strip()
    if not cleaned:
        return ""
    return cleaned.split(" ", 1)[0]


def _job_context(
    *,
    product_name: str,
    target_budget: float,
    max_price: float,
    listing_urls: list[str],
) -> dict[str, Any]:
    return {
        "product_name": product_name,
        "target_budget": target_budget,
        "max_price": max_price,
        "listing_urls": listing_urls,
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


def _build_worker_long_term(
    worker_name: str,
    agent_id: str,
    listing: dict[str, Any],
    *,
    job_context: dict[str, Any],
) -> dict[str, Any]:
    return {
        "role": worker_name,
        "agent_id": agent_id,
        "listing_url": listing.get("listing_url"),
        "stagehand_context_id": listing.get("stagehand_context_id")
        or listing.get("browserbase_session_id"),
        # Legacy key kept for backward compatibility in memory records.
        "browserbase_session_id": listing.get("stagehand_context_id")
        or listing.get("browserbase_session_id"),
        "product": listing.get("data") or {},
        "job": job_context,
    }


def _build_first_message_fallback(
    product: dict[str, Any],
    *,
    product_name: str,
) -> str:
    title = product.get("title") or product_name
    price = product.get("price") or "your listed price"
    seller_first_name = _first_name(product.get("seller_name"))
    location = product.get("location")
    greeting = f"Hi {seller_first_name}!" if seller_first_name else "Hi there!"
    location_clause = f" in {location}" if location else ""
    templates = [
        f"{greeting} I just came across your {title} listed at {price}{location_clause}. Is it still available?",
        f"{greeting} Your {title} for {price}{location_clause} caught my eye. Is it still available?",
        f"{greeting} I’m interested in your {title} priced at {price}{location_clause}. Is it still available?",
    ]
    return templates[len(str(title)) % len(templates)]


async def _generate_first_message(
    product: dict[str, Any],
    *,
    product_name: str,
    opening_offer: float | None = None,
) -> str:
    title = product.get("title") or product_name
    price = product.get("price") or "unknown"
    description = (product.get("description") or "").strip()
    seller_first_name = _first_name(product.get("seller_name"))
    location = product.get("location") or ""
    date_listed = product.get("date_listed") or "unknown"
    reason_for_selling = product.get("reason_for_selling") or "unknown"

    prompt = SystemMessage(
        content=f"""Write the first Facebook Marketplace message to a seller.

Listing details:
- Item: {title}
- Price: {price}
- Location: {location or "not listed"}
- Seller first name: {seller_first_name or "unknown"}
- Description: {description or "not provided"}
- Date listed text: {date_listed}
- Stated reason for selling: {reason_for_selling}
- Suggested first offer anchor: {f"${opening_offer:.0f}" if opening_offer is not None else "none"}

Rules:
- Be kind, warm, and genuinely interested — sound like a real person, not a bot
- Before writing, internally assess seller flexibility, motivation to sell quickly, listing staleness, and reason for selling
- Then choose ONE tactic from: anchoring low, bundling, flinch, walk-away, deadline with cash pickup
- Personalize using specific details from the listing (item name, location, description, staleness/reason cues)
- If a seller name is available, use ONLY the first name (never full name)
- Make this opener distinct and specific to this listing; avoid generic phrasing
- Keep it to 1-2 short sentences
- For deadline tactics, mention immediate pickup and cash
- Keep it realistic for a first message (do not be rude or overly aggressive)
- Do NOT use emojis
- Output ONLY the message text to send. No quotes, labels, or explanation."""
    )

    try:
        response = await model.ainvoke([prompt])
        message = str(response.content).strip().strip('"').strip("'")
        return message or _build_first_message_fallback(product, product_name=product_name)
    except Exception:
        return _build_first_message_fallback(product, product_name=product_name)


async def initialize_agents(
    *,
    product_name: str,
    target_budget: float,
    max_price: float,
    listing_urls: list[str],
    send_openers: bool = True,
    reply_wait_ms: int = 2500,
    event_publisher: AgentEventPublisher | None = None,
) -> dict[str, Any]:
    if not listing_urls:
        raise ValueError("At least one listing URL is required")
    if len(listing_urls) > 5:
        raise ValueError("At most 5 listing URLs are allowed")

    load_dotenv()

    worker_names = [f"worker{i}" for i in range(1, len(listing_urls) + 1)]
    worker_agent_ids = {name: name for name in worker_names}
    worker_session_ids: dict[str, str] = {}
    job_context = _job_context(
        product_name=product_name,
        target_budget=target_budget,
        max_price=max_price,
        listing_urls=listing_urls,
    )

    _log("Connecting to Redis…")
    memory = AgentMemoryStore()
    try:
        await asyncio.to_thread(memory.verify_connection)
    except redis.ConnectionError as exc:
        raise RuntimeError(
            "Cannot connect to Redis. Start local Redis with: docker compose up -d redis"
        ) from exc
    _log("Redis connected.")
    _emit_event(
        event_publisher,
        event_type="manager_bootstrap",
        actor_type="manager",
        actor_id="manager",
        summary=f"Manager initialized {len(worker_names)} workers",
        details={
            "worker_ids": worker_names,
            "target_budget": target_budget,
            "max_price": max_price,
        },
    )

    use_shared_context = _env_bool("STAGEHAND_USE_SHARED_CONTEXT", True)
    keep_windows_open = _env_bool("STAGEHAND_KEEP_WINDOWS_OPEN", True)
    _log(
        f"Scraping {len(worker_names)} listing(s) via local Stagehand "
        f"(shared_context={use_shared_context})…"
    )
    scraped = await initialize_browsers(
        listing_urls,
        max_concurrency=min(5, len(listing_urls)),
        scrape_timeout_s=120,
        use_shared_context=use_shared_context,
        keep_windows_open=keep_windows_open,
    )
    if len(scraped) != len(worker_names):
        raise RuntimeError(
            f"Expected {len(worker_names)} scraped listings, got {len(scraped)}"
        )

    assignments: list[dict[str, Any]] = []
    opening_messages: dict[str, str] = {}
    pending_openers: list[dict[str, Any]] = []

    _log("Writing agent memory to Redis…")
    await asyncio.to_thread(memory.init_market_floor)

    for worker_name, listing in zip(worker_names, scraped, strict=True):
        agent_id = worker_agent_ids[worker_name]
        session_id = listing.get("stagehand_context_id") or listing.get(
            "browserbase_session_id"
        )
        if not session_id:
            raise RuntimeError(f"Missing stagehand_context_id for {worker_name}")

        worker_session_ids[worker_name] = session_id
        product = listing.get("data") or {}
        listed_price = parse_price(product.get("price"))
        opening_offer = (
            max(target_budget, (listed_price or max_price) * 0.85)
            if listed_price
            else target_budget
        )

        long_term = _build_worker_long_term(
            worker_name,
            agent_id,
            listing,
            job_context=job_context,
        )
        await asyncio.to_thread(
            memory.init_memory,
            agent_id,
            long_term=long_term,
            short_term={
                "status": "initialized",
                "messages": [],
                "last_sent_message": None,
                "last_reply_message": None,
                "current_min_price": opening_offer,
            },
        )
        await asyncio.to_thread(
            memory.upsert_listing_vector,
            worker_id=worker_name,
            listing_url=listing.get("listing_url", ""),
            title=product.get("title") or product_name,
            description=product.get("description") or "",
            condition_text=(
                product.get("condition")
                or product.get("reason_for_selling")
                or product.get("date_listed")
                or ""
            ),
            listed_price=listed_price,
            current_best_price=opening_offer,
        )

        first_message = await _generate_first_message(
            product,
            product_name=product_name,
            opening_offer=opening_offer,
        )
        opening_messages[worker_name] = first_message
        _log(f"[{worker_name}] opener draft: {first_message}")
        _emit_event(
            event_publisher,
            event_type="worker_initialized",
            actor_type="worker",
            actor_id=worker_name,
            summary=f"{worker_name} initialized with listing context",
            details={
                "listing_url": listing.get("listing_url"),
                "listing_title": product.get("title") or product_name,
                "listed_price": product.get("price"),
                "opening_offer": opening_offer,
            },
        )
        _emit_event(
            event_publisher,
            event_type="worker_thinking",
            actor_type="worker",
            actor_id=worker_name,
            summary=f"{worker_name} drafted opening message",
            details={
                "phase": "opening_message",
                "thought": (
                    "Analyzed seller profile and listing cues to draft a personalized opener."
                ),
                "draft_message": first_message,
            },
        )
        opener_reply: str | None = None

        if send_openers:
            pending_openers.append(
                {
                    "worker_name": worker_name,
                    "agent_id": agent_id,
                    "session_id": session_id,
                    "listing_url": listing.get("listing_url"),
                    "first_message": first_message,
                    "opening_offer": opening_offer,
                }
            )

        assignments.append(
            {
                "worker_name": worker_name,
                "agent_id": agent_id,
                "stagehand_context_id": session_id,
                # Legacy key kept for compatibility with older consumers.
                "browserbase_session_id": session_id,
                "listing_url": listing.get("listing_url"),
                "product": product,
                "first_message": first_message,
                "opener_reply": opener_reply,
                "current_min_price": opening_offer,
            }
        )

        if listed_price is not None:
            await asyncio.to_thread(
                memory.set_global_lowest_price,
                listed_price,
                worker_id=worker_name,
                listing_url=listing.get("listing_url", ""),
            )

    if send_openers and pending_openers:
        shared_context = len({item["session_id"] for item in pending_openers}) == 1
        shared_browserbase_context = shared_context and str(
            pending_openers[0]["session_id"]
        ).startswith("browserbase-context:")
        if shared_context and not shared_browserbase_context:
            _log(
                f"Sending {len(pending_openers)} opener(s) sequentially "
                "to avoid context collisions…"
            )
            opener_parallelism = 1
        else:
            if shared_browserbase_context:
                _log(
                    f"Sending {len(pending_openers)} opener(s) concurrently "
                    "(shared Browserbase context mounted read-only)…"
                )
            else:
                _log(f"Sending {len(pending_openers)} opener(s) concurrently…")
            opener_parallelism = min(5, len(pending_openers))

        semaphore = asyncio.Semaphore(opener_parallelism)

        async def _send_single_opener(opener: dict[str, Any]) -> tuple[str, str | None]:
            worker_name = opener["worker_name"]
            async with semaphore:
                _log(f"[{worker_name}] sending opener…")
                _emit_event(
                    event_publisher,
                    event_type="manager_directive",
                    actor_type="manager",
                    actor_id="manager",
                    summary=f"Manager directed {worker_name} to send opener",
                    details={
                        "worker_id": worker_name,
                        "directive": "Send personalized opener and wait for seller reply",
                    },
                )
                _emit_event(
                    event_publisher,
                    event_type="message_sent",
                    actor_type="worker",
                    actor_id=worker_name,
                    summary=f"{worker_name} sent opening message",
                    details={
                        "to": "seller",
                        "message": opener["first_message"],
                        "phase": "opener",
                    },
                )
                chat = await send_listing_message(
                    opener["session_id"],
                    opener["first_message"],
                    listing_url=opener["listing_url"],
                    reply_wait_ms=reply_wait_ms,
                    skip_if_buyer_message_exists=True,
                )
                opener_reply = chat.get("reply_message")
                sent_message = chat.get("sent_message")
                if not sent_message:
                    _log(f"[{worker_name}] opener already present, skipping send.")
                    return worker_name, opener_reply
                await asyncio.to_thread(
                    memory.record_exchange,
                    opener["agent_id"],
                    sent_message=sent_message,
                    reply_message=opener_reply,
                    current_min_price=opener["opening_offer"],
                    status="negotiating",
                )
                _log(
                    f"[{worker_name}] opener sent"
                    + (
                        f", seller replied: {opener_reply!r}"
                        if opener_reply
                        else ", no reply yet"
                    )
                )
                if opener_reply:
                    _emit_event(
                        event_publisher,
                        event_type="message_received",
                        actor_type="worker",
                        actor_id=worker_name,
                        summary=f"{worker_name} received seller opener reply",
                        details={
                            "from": "seller",
                            "message": opener_reply,
                            "phase": "opener",
                        },
                    )
                return worker_name, opener_reply

        opener_results = await asyncio.gather(
            *(_send_single_opener(item) for item in pending_openers)
        )
        replies_by_worker = {worker: reply for worker, reply in opener_results}
        for item in assignments:
            worker = item["worker_name"]
            if worker in replies_by_worker:
                item["opener_reply"] = replies_by_worker[worker]

    print("\nAgent assignments initialized:")
    for item in assignments:
        print(f"\n  {item['worker_name']} -> {item['agent_id']}")
        print(f"    context id     : {item['stagehand_context_id']}")
        print(f"    listing        : {item['listing_url']}")
        print(f"    product        : {item['product'].get('title')} — {item['product'].get('price')}")
        print(f"    first message  : {item['first_message']}")
        if item.get("opener_reply"):
            print(f"    seller reply   : {item['opener_reply']}")

    floor = await asyncio.to_thread(memory.get_market_floor)
    if floor.get("lowest_price_seen") is not None:
        print(
            f"\n  Shared market floor: ${floor['lowest_price_seen']} "
            f"(from {floor.get('winning_worker_id')})"
        )

    return {
        "worker_agent_ids": worker_agent_ids,
        "worker_session_ids": worker_session_ids,
        "market_floor": floor,
        "assignments": assignments,
        "opening_messages": opening_messages,
    }


async def run_dealbot(
    *,
    product_name: str,
    target_budget: float,
    max_price: float,
    listing_urls: list[str],
    event_publisher: AgentEventPublisher | None = None,
) -> dict[str, Any]:
    from orchestration import launch_negotiation

    init_result = await initialize_agents(
        product_name=product_name,
        target_budget=target_budget,
        max_price=max_price,
        listing_urls=listing_urls,
        send_openers=True,
        event_publisher=event_publisher,
    )
    _log("Starting negotiation loops…")
    negotiation_results = await launch_negotiation(
        init_result["assignments"],
        event_publisher=event_publisher,
    )
    return {
        "initialization": init_result,
        "negotiation": negotiation_results,
    }


if __name__ == "__main__":
    asyncio.run(
        run_dealbot(
            product_name=DEFAULT_PRODUCT_NAME,
            target_budget=DEFAULT_TARGET_BUDGET,
            max_price=DEFAULT_MAX_PRICE,
            listing_urls=DEFAULT_LISTING_URLS,
        )
    )
