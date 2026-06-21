"""Redis-backed short/long-term memory and coordination primitives for Dealbot."""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from typing import Any

import redis
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage

try:
    from redisvl.index import SearchIndex
    from redisvl.query import VectorQuery
    from redisvl.utils.vectorize import HFTextVectorizer

    REDISVL_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency at runtime
    SearchIndex = None  # type: ignore[assignment]
    VectorQuery = None  # type: ignore[assignment]
    HFTextVectorizer = None  # type: ignore[assignment]
    REDISVL_AVAILABLE = False


DEFAULT_REDIS_URL = "redis://localhost:6379/0"
MARKET_FLOOR_KEY = "dealbot:shared:market_floor"
NEGOTIATION_EVENTS_CHANNEL = "dealbot:events:live"
NEGOTIATION_EVENTS_STREAM = "dealbot:events:stream"
LISTING_VECTOR_INDEX_NAME = "dealbot:listings:idx"
LISTING_VECTOR_DOC_PREFIX = "dealbot:listings"
LISTING_DOC_ID_MAP_KEY = "dealbot:listings:doc_id_map"


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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.split("#", 1)[0].strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _tokenize(text: str) -> list[str]:
    return [token.strip(".,!?;:()[]{}'\"") for token in text.lower().split() if token.strip()]


def _hashed_embedding(text: str, dims: int) -> list[float]:
    vector = [0.0] * max(1, dims)
    tokens = _tokenize(text)
    if not tokens:
        return vector
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        bucket = int.from_bytes(digest[:4], "big") % len(vector)
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vector[bucket] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if norm > 0:
        return [value / norm for value in vector]
    return vector


class AgentMemoryStore:
    SHORT_TERM_TTL_SECONDS = 60 * 60 * 24

    def __init__(self, client: redis.Redis | None = None):
        self._client = client or redis_client()
        self._vector_enabled = _env_bool("DEALBOT_ENABLE_VECTOR_SEARCH", True)
        self._vector_dims = int(os.environ.get("DEALBOT_VECTOR_DIMS", "384"))
        self._min_comparable_similarity = float(
            os.environ.get("DEALBOT_MIN_COMPARABLE_SIMILARITY", "0.78")
        )
        self._events_stream_maxlen = int(os.environ.get("DEALBOT_EVENT_STREAM_MAXLEN", "5000"))
        self._listing_index: SearchIndex | None = None
        self._vectorizer = None
        if self._vector_enabled:
            self._init_vectorizer()

    @staticmethod
    def _long_term_key(agent_id: str) -> str:
        return f"dealbot:agent:{agent_id}:long_term"

    @staticmethod
    def _short_term_key(agent_id: str) -> str:
        return f"dealbot:agent:{agent_id}:short_term"

    @staticmethod
    def _listing_lookup_key(worker_id: str, listing_url: str) -> str:
        return f"{worker_id}|{listing_url}"

    @staticmethod
    def _listing_doc_id(worker_id: str, listing_url: str) -> str:
        digest = hashlib.sha1(f"{worker_id}|{listing_url}".encode("utf-8")).hexdigest()
        return f"{worker_id}:{digest[:16]}"

    def _init_vectorizer(self) -> None:
        if not REDISVL_AVAILABLE or HFTextVectorizer is None:
            self._vectorizer = None
            return
        model_name = (
            os.environ.get("DEALBOT_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
            .strip()
            or "sentence-transformers/all-MiniLM-L6-v2"
        )
        try:
            self._vectorizer = HFTextVectorizer(model=model_name, dtype="float32")
        except Exception:
            self._vectorizer = None

    def _embed_text(self, text: str) -> list[float]:
        if self._vectorizer is not None:
            try:
                vector = self._vectorizer.embed(text)
                if hasattr(vector, "tolist"):
                    vector = vector.tolist()
                casted = [float(value) for value in vector]
                if casted and len(casted) != self._vector_dims:
                    self._vector_dims = len(casted)
                if casted:
                    return casted
            except Exception:
                pass
        return _hashed_embedding(text, self._vector_dims)

    def _listing_text(
        self,
        *,
        title: str,
        description: str,
        condition_text: str,
    ) -> str:
        return " | ".join(part.strip() for part in [title, description, condition_text] if part)

    def _ensure_listing_index(self) -> bool:
        if not self._vector_enabled or not REDISVL_AVAILABLE or SearchIndex is None:
            return False
        if self._listing_index is not None:
            return True

        schema = {
            "index": {
                "name": LISTING_VECTOR_INDEX_NAME,
                "prefix": LISTING_VECTOR_DOC_PREFIX,
                "storage_type": "json",
            },
            "fields": [
                {"name": "id", "type": "tag"},
                {"name": "worker_id", "type": "tag"},
                {"name": "listing_url", "type": "tag"},
                {"name": "title", "type": "text"},
                {"name": "description", "type": "text"},
                {"name": "condition", "type": "text"},
                {"name": "listed_price", "type": "numeric"},
                {"name": "latest_price", "type": "numeric"},
                {
                    "name": "embedding",
                    "type": "vector",
                    "attrs": {
                        "dims": self._vector_dims,
                        "distance_metric": "cosine",
                        "algorithm": "hnsw",
                        "datatype": "float32",
                    },
                },
            ],
        }
        try:
            index = SearchIndex.from_dict(
                schema,
                redis_client=self._client,
                validate_on_load=True,
            )
            if not index.exists():
                index.create()
            self._listing_index = index
            return True
        except Exception:
            self._listing_index = None
            return False

    def _emit_coordination_event(self, event_type: str, payload: dict[str, Any]) -> None:
        now_ms = int(time.time() * 1000)
        envelope = {
            "event_type": event_type,
            "published_at_ms": now_ms,
            **payload,
        }
        serialized = json.dumps(envelope, default=str)
        try:
            self._client.publish(NEGOTIATION_EVENTS_CHANNEL, serialized)
        except Exception:
            pass
        try:
            self._client.xadd(
                NEGOTIATION_EVENTS_STREAM,
                {
                    "event_type": event_type,
                    "payload": serialized,
                    "published_at_ms": str(now_ms),
                },
                maxlen=self._events_stream_maxlen,
                approximate=True,
            )
        except Exception:
            pass

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
        self._emit_coordination_event(
            "message_exchange",
            {
                "worker_id": agent_id,
                "status": status or "negotiating",
                "current_min_price": current_min_price,
                "sent_message": sent_message,
                "reply_message": reply_message,
            },
        )

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
        self._ensure_listing_index()

    def upsert_listing_vector(
        self,
        *,
        worker_id: str,
        listing_url: str,
        title: str,
        description: str,
        condition_text: str,
        listed_price: float | None,
        current_best_price: float | None = None,
    ) -> None:
        if not listing_url:
            return

        listing_text = self._listing_text(
            title=title,
            description=description,
            condition_text=condition_text,
        )
        embedding = self._embed_text(listing_text)
        if not self._ensure_listing_index() or self._listing_index is None:
            return

        listing_id = self._listing_doc_id(worker_id, listing_url)
        doc = {
            "id": listing_id,
            "worker_id": worker_id,
            "listing_url": listing_url,
            "title": title or "",
            "description": description or "",
            "condition": condition_text or "",
            "listed_price": listed_price,
            "latest_price": current_best_price if current_best_price is not None else listed_price,
            "embedding": embedding,
        }
        self._client.hset(
            LISTING_DOC_ID_MAP_KEY,
            self._listing_lookup_key(worker_id, listing_url),
            listing_id,
        )
        try:
            self._listing_index.load([doc], id_field="id")
        except Exception:
            return

        self._emit_coordination_event(
            "listing_indexed",
            {
                "worker_id": worker_id,
                "listing_url": listing_url,
                "title": title,
                "listed_price": listed_price,
            },
        )

    def update_listing_best_price(self, *, worker_id: str, listing_url: str, price: float) -> None:
        listing_id = self._client.hget(
            LISTING_DOC_ID_MAP_KEY,
            self._listing_lookup_key(worker_id, listing_url),
        )
        if not listing_id:
            return
        listing_key = f"{LISTING_VECTOR_DOC_PREFIX}:{listing_id}"
        try:
            self._client.json().set(listing_key, "$.latest_price", float(price))
        except Exception:
            pass

    def find_best_comparable_listing(
        self,
        *,
        worker_id: str,
        listing_url: str,
        title: str,
        description: str,
        condition_text: str,
        top_k: int = 4,
    ) -> dict[str, Any] | None:
        listing_text = self._listing_text(
            title=title,
            description=description,
            condition_text=condition_text,
        )
        if self._ensure_listing_index() and self._listing_index is not None and VectorQuery is not None:
            query_vector = self._embed_text(listing_text)
            try:
                query = VectorQuery(
                    vector=query_vector,
                    vector_field_name="embedding",
                    return_fields=[
                        "worker_id",
                        "listing_url",
                        "title",
                        "condition",
                        "latest_price",
                        "listed_price",
                        "vector_distance",
                    ],
                    num_results=max(2, top_k + 1),
                )
                results = self._listing_index.query(query)
            except Exception:
                results = []

            best: dict[str, Any] | None = None
            for item in results:
                candidate_worker = str(item.get("worker_id") or "")
                candidate_url = str(item.get("listing_url") or "")
                if not candidate_worker or not candidate_url:
                    continue
                if candidate_worker == worker_id and candidate_url == listing_url:
                    continue

                distance_raw = item.get("vector_distance")
                distance = parse_price(distance_raw)
                if distance is None:
                    continue
                similarity = max(0.0, 1.0 - distance)
                if similarity < self._min_comparable_similarity:
                    continue

                candidate_price = parse_price(item.get("latest_price"))
                if candidate_price is None:
                    candidate_price = parse_price(item.get("listed_price"))
                if candidate_price is None:
                    continue

                if best is None or candidate_price < float(best["price"]):
                    best = {
                        "price": candidate_price,
                        "worker_id": candidate_worker,
                        "listing_url": candidate_url,
                        "title": item.get("title"),
                        "condition": item.get("condition"),
                        "similarity": similarity,
                    }
            if best is not None:
                return best

        floor = self.get_market_floor()
        fallback_worker = floor.get("winning_worker_id")
        fallback_listing = floor.get("winning_listing_url")
        fallback_price = parse_price(floor.get("lowest_price_seen"))
        if fallback_price is None:
            return None
        if fallback_worker == worker_id and fallback_listing == listing_url:
            return None
        return {
            "price": fallback_price,
            "worker_id": fallback_worker,
            "listing_url": fallback_listing,
            "title": None,
            "condition": None,
            "similarity": None,
        }

    def open_live_event_subscription(self) -> Any:
        pubsub = self._client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(NEGOTIATION_EVENTS_CHANNEL)
        return pubsub

    def read_live_event(self, pubsub: Any, timeout_s: float = 0.2) -> dict[str, Any] | None:
        message = pubsub.get_message(timeout=timeout_s)
        if not message:
            return None
        if message.get("type") != "message":
            return None
        payload = message.get("data")
        if isinstance(payload, str):
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                return None
        return None

    def read_stream_events(
        self,
        *,
        last_id: str = "0-0",
        count: int = 100,
        block_ms: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        streams = self._client.xread(
            {NEGOTIATION_EVENTS_STREAM: last_id},
            count=count,
            block=block_ms,
        )
        parsed: list[tuple[str, dict[str, Any]]] = []
        for _, entries in streams:
            for message_id, fields in entries:
                payload = fields.get("payload")
                parsed_event: dict[str, Any]
                if isinstance(payload, str):
                    try:
                        parsed_event = json.loads(payload)
                    except json.JSONDecodeError:
                        parsed_event = {
                            "event_type": fields.get("event_type"),
                            "payload": payload,
                        }
                else:
                    parsed_event = {
                        "event_type": fields.get("event_type"),
                        "payload": payload,
                    }
                parsed.append((message_id, parsed_event))
        return parsed

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
        self.update_listing_best_price(worker_id=worker_id, listing_url=listing_url, price=price)

        floor = self.get_market_floor()
        current = floor.get("lowest_price_seen")
        if current is None or price < float(current):
            floor["lowest_price_seen"] = price
            floor["winning_listing_url"] = listing_url
            floor["winning_worker_id"] = worker_id
            self._client.set(MARKET_FLOOR_KEY, json.dumps(floor))
            self._emit_coordination_event(
                "price_improved",
                {
                    "worker_id": worker_id,
                    "listing_url": listing_url,
                    "new_price": price,
                },
            )
        else:
            self._emit_coordination_event(
                "price_seen",
                {
                    "worker_id": worker_id,
                    "listing_url": listing_url,
                    "observed_price": price,
                },
            )
