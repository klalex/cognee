"""Compatibility shims for the community Falkor hybrid adapter."""

from __future__ import annotations

import inspect
from typing import Callable

_PROVENANCE_KWARGS_PATCHED = False
_FILTERED_GRAPH_DATA_PATCHED = False
_RETRIEVE_PATCHED = False


def patch_falkor_adapter_graph_data() -> None:
    """Read logical endpoints from topology, not optional relationship properties.

    The community adapter's edge writer permits empty properties, but its
    reader requires source_node_id/target_node_id. Existing graphs therefore
    need a reader fix, not just changes to future writes.
    """
    try:
        from cognee_community_hybrid_adapter_falkor.falkor_adapter import FalkorDBAdapter
    except ImportError:
        return

    FalkorDBAdapter.get_graph_data = _get_graph_data


async def _get_graph_data(self):
    result = await self.query("MATCH (n) RETURN n.id, properties(n)")
    nodes = [(row[0], row[1]) for row in getattr(result, "result_set", result)]
    result = await self.query("MATCH (n)-[r]->(m) RETURN n.id, m.id, TYPE(r), properties(r)")
    edges = []
    for source_id, target_id, relationship_type, properties in getattr(
        result, "result_set", result
    ):
        properties = dict(properties or {})
        # Stored names may differ from the sanitized Cypher relationship type.
        relationship_name = properties.get("relationship_name") or relationship_type
        # Do not propagate stale denormalized endpoints during a migration.
        properties.update(source_node_id=source_id, target_node_id=target_id)
        edges.append((source_id, target_id, relationship_name, properties))
    return nodes, edges


def patch_falkor_adapter_provenance_kwargs() -> None:
    """Accept GraphDBInterface provenance kwargs that older Falkor adapters reject.

    Cognee's graph interface stamps ``source_ref_key`` / ``pipeline_run_id`` on
    ``add_nodes`` / ``add_edges``. Community Falkor adapters published before that
    change raise ``TypeError`` on those keyword arguments. Accept and ignore them
    until the adapter is updated upstream.
    """
    global _PROVENANCE_KWARGS_PATCHED
    if _PROVENANCE_KWARGS_PATCHED:
        return

    try:
        from cognee_community_hybrid_adapter_falkor.falkor_adapter import FalkorDBAdapter
    except Exception:
        return

    for method_name in ("add_nodes", "add_edges"):
        original = getattr(FalkorDBAdapter, method_name, None)
        if original is None:
            continue

        try:
            signature = inspect.signature(original)
        except (TypeError, ValueError):
            continue

        if "source_ref_key" in signature.parameters:
            continue

        setattr(FalkorDBAdapter, method_name, _wrap_ignore_provenance_kwargs(original))

    _PROVENANCE_KWARGS_PATCHED = True


def _wrap_ignore_provenance_kwargs(original: Callable):
    async def wrapped(self, items, source_ref_key=None, pipeline_run_id=None):
        return await original(self, items)

    wrapped.__name__ = getattr(original, "__name__", "wrapped")
    wrapped.__doc__ = getattr(original, "__doc__", None)
    return wrapped


def patch_falkor_adapter_filtered_graph_data() -> None:
    """Accept ``include_edges`` on older community Falkor ``get_filtered_graph_data``.

    Cognee's GraphDBInterface added a keyword-only ``include_edges`` flag so
    callers can skip the edge query (e.g. conflict resolution only needs
    ``FactConflict`` nodes). Older community Falkor adapters reject that kwarg
    with ``TypeError``. Accept it here until the adapter catches up; when
    ``include_edges`` is False, drop the edge half of the result.
    """
    global _FILTERED_GRAPH_DATA_PATCHED
    if _FILTERED_GRAPH_DATA_PATCHED:
        return

    try:
        from cognee_community_hybrid_adapter_falkor.falkor_adapter import FalkorDBAdapter
    except Exception:
        return

    original = getattr(FalkorDBAdapter, "get_filtered_graph_data", None)
    if original is None:
        return

    try:
        signature = inspect.signature(original)
    except (TypeError, ValueError):
        return

    if "include_edges" in signature.parameters:
        _FILTERED_GRAPH_DATA_PATCHED = True
        return

    setattr(FalkorDBAdapter, "get_filtered_graph_data", _wrap_include_edges(original))
    _FILTERED_GRAPH_DATA_PATCHED = True


def patch_falkor_adapter_retrieve() -> None:
    """Return vector lookup rows as scored results, not raw Falkor result rows.

    ``FalkorDBAdapter.retrieve`` hands back ``QueryResult.result_set``. Each
    row is a list whose first cell is the node, so ``row.id`` raises
    ``AttributeError: 'list' object has no attribute 'id'``. Cognify hits
    that in ``find_indexed_chunks`` after the indexing pass, and the remember
    run fails before entity extraction. Other vector adapters return
    ``ScoredResult``. Wrap the community method until it does too. Rows that
    already have ``id`` pass through, so a fixed adapter is left alone.
    """
    global _RETRIEVE_PATCHED
    if _RETRIEVE_PATCHED:
        return

    try:
        from cognee_community_hybrid_adapter_falkor.falkor_adapter import FalkorDBAdapter
    except Exception:
        return

    original = getattr(FalkorDBAdapter, "retrieve", None)
    if original is None:
        return

    setattr(FalkorDBAdapter, "retrieve", _wrap_retrieve(original))
    _RETRIEVE_PATCHED = True


def _node_properties(cell) -> dict:
    properties = getattr(cell, "properties", None)
    if isinstance(properties, dict):
        return dict(properties)
    if isinstance(cell, dict):
        return dict(cell)
    return {}


def _collection_vector_key(collection_name: str | None) -> str | None:
    if not collection_name or "_" not in collection_name:
        return None
    attribute = collection_name.partition("_")[2]
    return f"{attribute}_vector" if attribute else None


def _as_float_vector(value):
    if value is None:
        return None
    if isinstance(value, str):
        start, end = value.find("["), value.rfind("]")
        if start < 0 or end <= start:
            return None
        try:
            return [float(part) for part in value[start + 1 : end].split(",") if part.strip()]
        except ValueError:
            return None
    if hasattr(value, "tolist"):
        value = value.tolist()
    try:
        vector = [float(item) for item in value]
    except (TypeError, ValueError):
        return None
    return vector or None


def _take_stored_vector(payload: dict, collection_name: str | None) -> list[float] | None:
    """Move a Falkor ``*_vector`` property to the ``vector`` key other adapters use."""
    preferred = _collection_vector_key(collection_name)
    keys = [preferred] if preferred and preferred in payload else []
    keys.extend(key for key in payload if key.endswith("_vector") and key not in keys)
    vector = None
    for key in keys:
        vector = _as_float_vector(payload.get(key))
        if vector:
            break
    for key in list(payload):
        if key.endswith("_vector"):
            payload.pop(key, None)
    return vector


def _scored_retrieve_rows(rows, *, collection_name: str | None = None, include_vector: bool = False):
    from cognee.infrastructure.databases.vector.models.ScoredResult import ScoredResult
    from cognee.infrastructure.engine.utils.parse_id import parse_id

    if not rows:
        return []

    scored = []
    for row in rows:
        if not isinstance(row, (list, tuple)):
            if include_vector and isinstance(getattr(row, "payload", None), dict):
                if row.payload.get("vector") is None:
                    vector = _take_stored_vector(row.payload, collection_name)
                    if vector is not None:
                        row.payload["vector"] = vector
            scored.append(row)
            continue
        cell = row[0] if row else None
        payload = _node_properties(cell)
        node_id = payload.get("id")
        if node_id is None and cell is not None:
            node_id = getattr(cell, "id", None)
        if node_id is None:
            continue
        if include_vector:
            vector = _take_stored_vector(payload, collection_name)
            if vector is not None:
                payload["vector"] = vector
        scored.append(ScoredResult(id=parse_id(node_id), payload=payload, score=0))
    return scored


def _wrap_retrieve(original: Callable):
    try:
        accepts_include_vector = "include_vector" in inspect.signature(original).parameters
    except (TypeError, ValueError):
        accepts_include_vector = False

    async def wrapped(self, collection_name, data_point_ids, *args, include_vector=False, **kwargs):
        if accepts_include_vector:
            rows = await original(
                self,
                collection_name,
                data_point_ids,
                *args,
                include_vector=include_vector,
                **kwargs,
            )
        else:
            rows = await original(self, collection_name, data_point_ids, *args, **kwargs)
        return _scored_retrieve_rows(
            rows, collection_name=collection_name, include_vector=include_vector
        )

    wrapped.__name__ = getattr(original, "__name__", "wrapped")
    wrapped.__doc__ = getattr(original, "__doc__", None)
    return wrapped


def _wrap_include_edges(original: Callable):
    async def wrapped(self, attribute_filters, *, include_edges: bool = True):
        nodes, edges = await original(self, attribute_filters)
        if include_edges:
            return nodes, edges
        return nodes, []

    wrapped.__name__ = getattr(original, "__name__", "wrapped")
    wrapped.__doc__ = getattr(original, "__doc__", None)
    return wrapped
