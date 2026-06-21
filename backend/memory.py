"""Redis-backed short/long-term memory for Dealbot agents."""

from __future__ import annotations

import json
import os
from typing import Any

import redis
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage


DEFAULT_REDIS_URL = "redis://localhost:6379/0"
MARKET_FLOOR_KEY = "dealbot:shared:market_floor"


def redis_client() -> redis.Redis:
    url = os.environ.get("REDIS_URL", DEFAULT_REDIS_URL).strip() or DEFAULT_REDIS_URL
    return redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


def parse_price(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


class AgentMemoryStore:
    SHORT_TERM_TTL_SECONDS = 60 * 60 * 24

    def __init__(self, client: redis.Redis | None = None):
        self._client = client or redis_client()

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
            "current_min_price": None,
        }
        self._client.set(self._long_term_key(agent_id), json.dumps(long_term))
        self._save_short_term(agent_id, short_payload)

    def get_long_term(self, agent_id: str) -> dict[str, Any]:
        raw = self._client.get(self._long_term_key(agent_id))
        return json.loads(raw) if raw else {}

    def get_short_term(self, agent_id: str) -> dict[str, Any]:
        raw = self._client.get(self._short_term_key(agent_id))
        return json.loads(raw) if raw else {}

    def _save_short_term(self, agent_id: str, payload: dict[str, Any]) -> None:
        self._client.setex(
            self._short_term_key(agent_id),
            self.SHORT_TERM_TTL_SECONDS,
            json.dumps(payload),
        )

    def update_short_term(self, agent_id: str, **updates: Any) -> dict[str, Any]:
        payload = self.get_short_term(agent_id)
        payload.update(updates)
        self._save_short_term(agent_id, payload)
        return payload

    def append_chat_messages(
        self,
        agent_id: str,
        entries: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        short_term = self.get_short_term(agent_id)
        messages = list(short_term.get("messages") or [])
        messages.extend(entries)
        short_term["messages"] = messages
        if entries:
            last = entries[-1]
            if last["role"] == "assistant":
                short_term["last_sent_message"] = last["content"]
            elif last["role"] == "user":
                short_term["last_reply_message"] = last["content"]
        self._save_short_term(agent_id, short_term)
        return messages

    def record_exchange(
        self,
        agent_id: str,
        *,
        sent_message: str,
        reply_message: str | None,
        current_min_price: float | None = None,
        status: str | None = None,
    ) -> None:
        entries = [{"role": "assistant", "content": sent_message}]
        if reply_message:
            entries.append({"role": "user", "content": reply_message})
        self.append_chat_messages(agent_id, entries)
        updates: dict[str, Any] = {
            "last_sent_message": sent_message,
            "last_reply_message": reply_message,
        }
        if current_min_price is not None:
            updates["current_min_price"] = current_min_price
        if status is not None:
            updates["status"] = status
        self.update_short_term(agent_id, **updates)

    def get_chat_messages(self, agent_id: str) -> list[dict[str, str]]:
        return list(self.get_short_term(agent_id).get("messages") or [])

    def to_langchain_messages(self, agent_id: str) -> list[AnyMessage]:
        converted: list[AnyMessage] = []
        for entry in self.get_chat_messages(agent_id):
            role = entry.get("role")
            content = entry.get("content", "")
            if role == "assistant":
                converted.append(AIMessage(content=content))
            elif role == "user":
                converted.append(HumanMessage(content=content))
        return converted

    def init_market_floor(self) -> None:
        self._client.set(
            MARKET_FLOOR_KEY,
            json.dumps(
                {
                    "lowest_price_seen": None,
                    "winning_worker_id": None,
                    "winning_listing_url": None,
                }
            ),
        )

    def get_market_floor(self) -> dict[str, Any]:
        raw = self._client.get(MARKET_FLOOR_KEY)
        return json.loads(raw) if raw else {}

    def get_global_lowest_price(self) -> float | None:
        value = self.get_market_floor().get("lowest_price_seen")
        return float(value) if value is not None else None

    def set_global_lowest_price(
        self,
        price: float,
        *,
        worker_id: str,
        listing_url: str,
    ) -> None:
        floor = self.get_market_floor()
        current = floor.get("lowest_price_seen")
        if current is None or price < float(current):
            floor["lowest_price_seen"] = price
            floor["winning_listing_url"] = listing_url
            floor["winning_worker_id"] = worker_id
            self._client.set(MARKET_FLOOR_KEY, json.dumps(floor))
