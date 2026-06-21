"""
Assign worker/manager agents to Marketplace listings, persist memory in Redis,
scrape listing data via browserb, and draft opening seller messages.

Local Redis (Docker):
  docker compose up -d redis

Optional .env override:
  REDIS_URL=redis://localhost:6379/0
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import redis
from dotenv import load_dotenv

from browserb import initialize_browsers, send_listing_message


# ---------------------------------------------------------------------------
# Job configuration
# ---------------------------------------------------------------------------

PRODUCT_NAME = "Gray Couch"
TARGET_BUDGET = 40
MAX_PRICE = 50

LISTING_URLS = [
    "https://www.facebook.com/marketplace/item/954225447099368/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
    "https://www.facebook.com/marketplace/item/1346267354014284/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
    "https://www.facebook.com/marketplace/item/954225447099368/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
    "https://www.facebook.com/marketplace/item/1346267354014284/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
    "https://www.facebook.com/marketplace/item/954225447099368/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
]

WORKER_NAMES = ["worker1", "worker2", "worker3", "worker4", "worker5"]
MANAGER_AGENT_ID = "manager"

# Populated during initialize_agents()
WORKER_AGENT_IDS: dict[str, str] = {name: name for name in WORKER_NAMES}
WORKER_SESSION_IDS: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Redis (local Docker by default)
# ---------------------------------------------------------------------------

DEFAULT_REDIS_URL = "redis://localhost:6379/0"


def _redis_client() -> redis.Redis:
    url = os.environ.get("REDIS_URL", DEFAULT_REDIS_URL).strip() or DEFAULT_REDIS_URL
    return redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


def _log(message: str) -> None:
    print(message, flush=True)


class AgentMemoryStore:
    """Short- and long-term memory stored in Redis."""

    SHORT_TERM_TTL_SECONDS = 60 * 60 * 24  # 24 hours

    def __init__(self, client: redis.Redis):
        self._client = client

    @staticmethod
    def _long_term_key(agent_id: str) -> str:
        return f"dealbot:agent:{agent_id}:long_term"

    @staticmethod
    def _short_term_key(agent_id: str) -> str:
        return f"dealbot:agent:{agent_id}:short_term"

    def verify_connection(self) -> None:
        self._client.ping()

    def init_memory(
        self,
        agent_id: str,
        *,
        long_term: dict[str, Any],
        short_term: dict[str, Any] | None = None,
    ) -> None:
        short_payload = short_term or {
            "status": "initialized",
            "messages": [],
            "last_sent_message": None,
            "last_reply_message": None,
        }
        self._client.set(self._long_term_key(agent_id), json.dumps(long_term))
        self._client.setex(
            self._short_term_key(agent_id),
            self.SHORT_TERM_TTL_SECONDS,
            json.dumps(short_payload),
        )

    def get_long_term(self, agent_id: str) -> dict[str, Any]:
        raw = self._client.get(self._long_term_key(agent_id))
        return json.loads(raw) if raw else {}

    def get_short_term(self, agent_id: str) -> dict[str, Any]:
        raw = self._client.get(self._short_term_key(agent_id))
        return json.loads(raw) if raw else {}


def _job_context() -> dict[str, Any]:
    return {
        "product_name": PRODUCT_NAME,
        "target_budget": TARGET_BUDGET,
        "max_price": MAX_PRICE,
        "listing_urls": LISTING_URLS,
    }


def _build_worker_long_term(
    worker_name: str,
    agent_id: str,
    listing: dict[str, Any],
) -> dict[str, Any]:
    return {
        "role": worker_name,
        "agent_id": agent_id,
        "listing_url": listing.get("listing_url"),
        "browserbase_session_id": listing.get("browserbase_session_id"),
        "product": listing.get("data") or {},
        "job": _job_context(),
    }


def _build_manager_long_term(
    agent_id: str,
    assignments: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "role": "manager",
        "agent_id": agent_id,
        "job": _job_context(),
        "assignments": assignments,
        "lowest_price_seen": None,
        "winning_listing_url": None,
    }


def _build_first_message(product: dict[str, Any]) -> str:
    title = product.get("title") or PRODUCT_NAME
    price = product.get("price") or "your listed price"
    location = product.get("location")

    if location:
        return (
            f"Hey! I saw your {title} for {price} in {location} and it looks great. "
            f"Is it still available?"
        )
    return (
        f"Hey! I saw your {title} for {price} and it looks great. "
        f"Is it still available?"
    )


async def initialize_agents() -> dict[str, Any]:
    """
    Scrape listings, assign one worker per listing, initialize Redis memory
    for all 6 agents, and draft opening seller messages.
    """
    global WORKER_SESSION_IDS

    load_dotenv()

    _log("Connecting to Redis…")
    memory = AgentMemoryStore(_redis_client())
    try:
        await asyncio.to_thread(memory.verify_connection)
    except redis.ConnectionError as exc:
        raise RuntimeError(
            "Cannot connect to Redis at "
            f"{os.environ.get('REDIS_URL', DEFAULT_REDIS_URL)}. "
            "Start local Redis with: docker compose up -d redis"
        ) from exc
    _log("Redis connected.")

    _log(f"Scraping {len(WORKER_NAMES)} listing(s) via Browserbase…")
    scraped = await initialize_browsers(
        LISTING_URLS[: len(WORKER_NAMES)],
        max_concurrency=2,
        scrape_timeout_s=120,
    )
    if len(scraped) != len(WORKER_NAMES):
        raise RuntimeError(
            f"Expected {len(WORKER_NAMES)} scraped listings, got {len(scraped)}"
        )

    assignments: list[dict[str, Any]] = []
    opening_messages: dict[str, str] = {}

    _log("Writing agent memory to Redis…")
    for worker_name, listing in zip(WORKER_NAMES, scraped, strict=True):
        agent_id = WORKER_AGENT_IDS[worker_name]
        session_id = listing.get("browserbase_session_id")

        if not session_id:
            raise RuntimeError(f"Missing browserbase_session_id for {worker_name}")

        WORKER_SESSION_IDS[worker_name] = session_id

        long_term = _build_worker_long_term(worker_name, agent_id, listing)
        await asyncio.to_thread(memory.init_memory, agent_id, long_term=long_term)

        product = listing.get("data") or {}
        first_message = _build_first_message(product)
        opening_messages[worker_name] = first_message

        assignments.append(
            {
                "worker_name": worker_name,
                "agent_id": agent_id,
                "browserbase_session_id": session_id,
                "listing_url": listing.get("listing_url"),
                "product": product,
                "first_message": first_message,
            }
        )

    await asyncio.to_thread(
        memory.init_memory,
        MANAGER_AGENT_ID,
        long_term=_build_manager_long_term(MANAGER_AGENT_ID, assignments),
        short_term={
            "status": "initialized",
            "active_workers": WORKER_NAMES,
            "broadcasts": [],
            "messages": [],
        },
    )

    print("\nAgent assignments initialized:")
    print(f"  Manager agent id : {MANAGER_AGENT_ID}")
    for item in assignments:
        print(f"\n  {item['worker_name']} -> {item['agent_id']}")
        print(f"    session id     : {item['browserbase_session_id']}")
        print(f"    listing        : {item['listing_url']}")
        print(f"    product        : {item['product'].get('title')} — {item['product'].get('price')}")
        print(f"    first message  : {item['first_message']}")

        # chat = await send_listing_message(
        #     item["browserbase_session_id"],
        #     item["first_message"],
        # )
        # print(f"    seller reply   : {chat['reply_message']}")

    return {
        "manager_agent_id": MANAGER_AGENT_ID,
        "worker_agent_ids": dict(WORKER_AGENT_IDS),
        "worker_session_ids": dict(WORKER_SESSION_IDS),
        "assignments": assignments,
        "opening_messages": opening_messages,
    }


if __name__ == "__main__":
    asyncio.run(initialize_agents())
