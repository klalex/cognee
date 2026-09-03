"""Compatibility shims for the community Falkor hybrid adapter."""

from __future__ import annotations

import inspect
from typing import Callable

_PROVENANCE_KWARGS_PATCHED = False


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
