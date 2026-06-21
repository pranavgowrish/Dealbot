"""Agent-graph span helpers.

The negotiation pipeline in ``orchestration.py`` is a hand-rolled asyncio loop
rather than a compiled LangGraph ``StateGraph``, so the LangChain instrumentor
has no node structure to emit and Arize's "Agent Graph" view stays empty
("Add agent metadata to view graph").

This module tags each logical pipeline step with the OpenInference graph
semantic-convention attributes (``graph.node.id`` / ``graph.node.name`` /
``graph.node.parent_id``) that Arize aggregates into the agent graph. Edges are
drawn from ``parent_id -> id`` across *all* traces, independent of span nesting,
so a closed manager span still anchors the worker nodes that name it as parent.
Best-effort: if the OTel/OpenInference libs are missing the decorator is a no-op.
"""
from __future__ import annotations

import functools
from contextlib import contextmanager
from typing import Any, Callable, Iterator

try:
    from openinference.semconv.trace import (
        OpenInferenceSpanKindValues,
        SpanAttributes,
    )
    from opentelemetry import trace

    _tracer = trace.get_tracer("dealbot.agents")
    _AGENT = OpenInferenceSpanKindValues.AGENT.value
    _CHAIN = OpenInferenceSpanKindValues.CHAIN.value
    _ENABLED = True
except Exception:  # noqa: BLE001 - never let graph tagging break the pipeline
    _tracer = None
    _AGENT = "AGENT"
    _CHAIN = "CHAIN"
    _ENABLED = False

# Public span-kind constants for callers that want a non-default node kind.
AGENT = _AGENT
CHAIN = _CHAIN


@contextmanager
def round_span(name: str, **attributes: Any) -> Iterator[Any]:
    """Open a root span so every node in one negotiation round shares a trace.

    Arize's per-trace Agent Graph view assembles the graph from the
    ``graph.node.*`` spans *within a single trace*. The pipeline runs each step
    as its own asyncio task, which would otherwise emit one trace per node (a
    single, parent-less node Arize can't draw). Wrapping the round in this root
    span — entered before the worker fan-out — makes the gathered tasks inherit
    its context, so manager + all worker nodes land in one connected trace.
    """
    if not _ENABLED:
        yield None
        return
    attrs = {SpanAttributes.OPENINFERENCE_SPAN_KIND: _CHAIN, **attributes}
    with _tracer.start_as_current_span(name, attributes=attrs) as span:
        yield span


def _worker_id_from_args(args: tuple, kwargs: dict) -> str | None:
    candidate = args[0] if args else kwargs.get("state")
    if isinstance(candidate, dict):
        worker_id = candidate.get("worker_id")
        return str(worker_id) if worker_id else None
    return None


def agent_node(
    node_id: str,
    *,
    parent_id: str | None = None,
    name: str | None = None,
    kind: str = _AGENT,
) -> Callable:
    """Decorator: run an async pipeline step inside a graph-node span.

    ``node_id`` is the logical node key (shared across workers, so the graph
    shows one "create_and_send" node rather than one per worker). ``parent_id``
    names the upstream node to draw the edge from.
    """

    def decorator(fn: Callable) -> Callable:
        if not _ENABLED:
            return fn

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            attributes: dict[str, Any] = {
                SpanAttributes.OPENINFERENCE_SPAN_KIND: kind,
                SpanAttributes.GRAPH_NODE_ID: node_id,
                SpanAttributes.GRAPH_NODE_NAME: name or node_id,
            }
            if parent_id:
                attributes[SpanAttributes.GRAPH_NODE_PARENT_ID] = parent_id
            worker_id = _worker_id_from_args(args, kwargs)
            if worker_id:
                attributes["worker_id"] = worker_id
            with _tracer.start_as_current_span(name or node_id, attributes=attributes):
                return await fn(*args, **kwargs)

        return wrapper

    return decorator
