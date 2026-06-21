"""MQTT event publisher for live Dealbot agent telemetry."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

try:
    import paho.mqtt.client as mqtt
except Exception:  # pragma: no cover - optional runtime dependency
    mqtt = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.split("#", 1)[0].strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class AgentEventPublisher:
    """Publishes job-scoped orchestration events to MQTT."""

    job_id: str
    enabled: bool = field(
        default_factory=lambda: _env_bool("DEALBOT_MQTT_ENABLED", True)
    )
    topic_root: str = field(
        default_factory=lambda: (
            os.environ.get("DEALBOT_MQTT_TOPIC_ROOT", "dealbot/agent-events")
            .strip()
            .strip("/")
        )
    )
    host: str = field(
        default_factory=lambda: os.environ.get("DEALBOT_MQTT_HOST", "localhost").strip()
        or "localhost"
    )
    port: int = field(default_factory=lambda: int(os.environ.get("DEALBOT_MQTT_PORT", "1883")))
    qos: int = field(default_factory=lambda: int(os.environ.get("DEALBOT_MQTT_QOS", "0")))
    keepalive_s: int = field(
        default_factory=lambda: int(os.environ.get("DEALBOT_MQTT_KEEPALIVE_S", "45"))
    )
    username: str | None = field(
        default_factory=lambda: os.environ.get("DEALBOT_MQTT_USERNAME", "").strip() or None
    )
    password: str | None = field(
        default_factory=lambda: os.environ.get("DEALBOT_MQTT_PASSWORD", "").strip() or None
    )
    use_tls: bool = field(default_factory=lambda: _env_bool("DEALBOT_MQTT_TLS", False))
    _client: Any = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _disabled_reason: str | None = field(default=None, init=False, repr=False)
    _connected: bool = field(default=False, init=False, repr=False)

    @property
    def topic(self) -> str:
        return f"{self.topic_root}/{self.job_id}"

    def _disable(self, reason: str) -> None:
        if self._disabled_reason:
            return
        self._disabled_reason = reason
        logger.warning("MQTT telemetry disabled: %s", reason)

    def _ensure_connected_locked(self) -> bool:
        if not self.enabled:
            return False
        if self._disabled_reason:
            return False
        if mqtt is None:
            self._disable("paho-mqtt dependency missing")
            return False
        if self._connected and self._client is not None:
            return True
        try:
            client_id = f"dealbot-{self.job_id[:8]}-{uuid.uuid4().hex[:6]}"
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
                clean_session=True,
                protocol=mqtt.MQTTv311,
                transport="tcp",
            )
            if self.username:
                client.username_pw_set(self.username, self.password)
            if self.use_tls:
                client.tls_set()
            client.connect(self.host, self.port, keepalive=self.keepalive_s)
            client.loop_start()
        except Exception as exc:
            self._disable(f"connect failed ({exc})")
            return False
        self._client = client
        self._connected = True
        return True

    def publish(
        self,
        event_type: str,
        *,
        actor_type: str,
        actor_id: str,
        summary: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled:
            return
        envelope = {
            "event_id": str(uuid.uuid4()),
            "job_id": self.job_id,
            "event_type": event_type,
            "actor_type": actor_type,
            "actor_id": actor_id,
            "summary": summary,
            "details": details or {},
            "published_at_ms": int(time.time() * 1000),
        }
        payload = json.dumps(envelope, default=str)

        with self._lock:
            if not self._ensure_connected_locked():
                return
            try:
                info = self._client.publish(
                    self.topic,
                    payload=payload,
                    qos=max(0, min(self.qos, 2)),
                    retain=False,
                )
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    self._disable(f"publish failed rc={info.rc}")
            except Exception as exc:
                self._disable(f"publish exception ({exc})")

    def close(self) -> None:
        with self._lock:
            client = self._client
            self._client = None
            self._connected = False
        if client is None:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass
