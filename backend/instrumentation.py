# instrumentation.py
# OpenInference tracing for the LangChain/LangGraph negotiation. Dual-mode:
#
#   * Arize AX (cloud)  -> when ARIZE_SPACE_ID and ARIZE_API_KEY are set.
#                          Restores the hosted dashboards (Spans, Agent Graph,
#                          Agent Path) at app.arize.com.
#   * Phoenix (local)   -> otherwise. Local UI at http://localhost:6006.
#
# Either way the SAME LangChainInstrumentor is used, so the captured spans (and
# therefore the agent-graph / agent-path views) are identical — only the
# destination differs. Best-effort: never raises if tracing can't start.
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

tracer_provider = None
TRACING_BACKEND: str | None = None


def _disabled() -> bool:
    return os.environ.get("DEALBOT_TRACING_ENABLED", "true").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }


def _register_arize():
    """Arize AX cloud — restores Spans / Agent Graph / Agent Path dashboards.

    Defaults to OTLP-HTTP transport: gRPC (the library default) rides HTTP/2 and
    is blocked/mangled by some proxy networks (same issue that blocks MQTT/TCP
    here), surfacing as 'StatusCode.UNAVAILABLE' export failures. HTTP/protobuf is
    a plain HTTPS POST and gets through. Override with ARIZE_TRANSPORT=grpc.
    """
    from arize.otel import Transport, register

    name = os.environ.get("ARIZE_TRANSPORT", "http").strip().lower()
    transport = {
        "grpc": Transport.GRPC,
        "http": Transport.HTTP,
        "https": Transport.HTTPS,
    }.get(name, Transport.HTTP)

    kwargs = dict(
        space_id=os.environ["ARIZE_SPACE_ID"],
        api_key=os.environ["ARIZE_API_KEY"],
        project_name=os.environ.get("ARIZE_PROJECT_NAME", "dealbot"),
        transport=transport,
    )
    # gRPC resolves the default Endpoint enum itself; the HTTP exporter needs an
    # explicit traces URL (otherwise it tries to POST to the literal enum name).
    if transport is not Transport.GRPC:
        kwargs["endpoint"] = os.environ.get(
            "ARIZE_ENDPOINT", "https://otlp.arize.com/v1/traces"
        )

    return register(**kwargs)


def _register_phoenix():
    """Local Phoenix collector."""
    from phoenix.otel import register

    endpoint = os.environ.get(
        "PHOENIX_COLLECTOR_ENDPOINT", "http://localhost:6006"
    ).rstrip("/")
    return register(
        project_name=os.environ.get("PHOENIX_PROJECT_NAME", "dealbot"),
        endpoint=f"{endpoint}/v1/traces",
        protocol="http/protobuf",
        auto_instrument=False,
        set_global_tracer_provider=True,
        verbose=False,
    )


def init_tracing() -> None:
    global tracer_provider, TRACING_BACKEND

    if _disabled():
        print("[tracing] disabled via DEALBOT_TRACING_ENABLED", flush=True)
        return

    use_arize = bool(
        os.environ.get("ARIZE_SPACE_ID") and os.environ.get("ARIZE_API_KEY")
    )

    try:
        if use_arize:
            tracer_provider = _register_arize()
            TRACING_BACKEND = "arize"
            target = "Arize AX cloud (app.arize.com)"
        else:
            tracer_provider = _register_phoenix()
            TRACING_BACKEND = "phoenix"
            target = os.environ.get(
                "PHOENIX_COLLECTOR_ENDPOINT", "http://localhost:6006"
            )

        from openinference.instrumentation.langchain import LangChainInstrumentor

        LangChainInstrumentor().instrument(tracer_provider=tracer_provider)
        print(
            f"[tracing] {TRACING_BACKEND} initialized -> {target} "
            f"(project={os.environ.get('ARIZE_PROJECT_NAME') or os.environ.get('PHOENIX_PROJECT_NAME', 'dealbot')})",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001 - never let tracing break the app
        tracer_provider = None
        TRACING_BACKEND = None
        print(f"[tracing] disabled: {exc}", flush=True)


init_tracing()
