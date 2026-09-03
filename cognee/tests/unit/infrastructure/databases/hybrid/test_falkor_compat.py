"""Compatibility patches for older community Falkor adapters."""

import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from cognee.infrastructure.databases.hybrid.falkor import compat


@pytest.mark.asyncio
async def test_provenance_kwargs_wrapper_ignores_extra_args():
    calls = []

    async def original(self, items):
        calls.append(items)
        return "ok"

    wrapped = compat._wrap_ignore_provenance_kwargs(original)
    result = await wrapped(
        SimpleNamespace(),
        ["node-1"],
        source_ref_key="dataset:data",
        pipeline_run_id="run-1",
    )

    assert result == "ok"
    assert calls == [["node-1"]]


@pytest.mark.asyncio
async def test_patch_accepts_provenance_kwargs_on_legacy_adapter(monkeypatch):
    class LegacyAdapter:
        async def add_nodes(self, nodes):
            return len(nodes)

        async def add_edges(self, edges):
            return len(edges)

    parent = types.ModuleType("cognee_community_hybrid_adapter_falkor")
    parent.__path__ = []
    child = types.ModuleType("cognee_community_hybrid_adapter_falkor.falkor_adapter")
    child.FalkorDBAdapter = LegacyAdapter

    monkeypatch.setitem(sys.modules, "cognee_community_hybrid_adapter_falkor", parent)
    monkeypatch.setitem(sys.modules, "cognee_community_hybrid_adapter_falkor.falkor_adapter", child)
    monkeypatch.setattr(compat, "_PROVENANCE_KWARGS_PATCHED", False)

    compat.patch_falkor_adapter_provenance_kwargs()

    adapter = LegacyAdapter()
    assert await adapter.add_nodes(["a"], source_ref_key="sr", pipeline_run_id="run") == 1
    assert await adapter.add_edges([("a", "b", "R", {})], source_ref_key="sr") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_result", [False, True])
async def test_graph_reader_uses_logical_topology_and_preserves_properties(
    monkeypatch, legacy_result
):
    class Adapter:
        pass

    child = types.ModuleType("cognee_community_hybrid_adapter_falkor.falkor_adapter")
    child.FalkorDBAdapter = Adapter
    monkeypatch.setitem(sys.modules, child.__name__, child)
    compat.patch_falkor_adapter_graph_data()
    compat.patch_falkor_adapter_graph_data()  # repeated factory calls are safe

    props = {
        "source_node_id": "stale-source",
        "target_node_id": "stale-target",
        "relationship_name": "made from",
        "weight": 0.7,
    }
    results = [
        [["logical-a", {"id": "logical-a"}], ["logical-b", {"id": "logical-b"}]],
        [
            ["logical-a", "logical-b", "contains", {}],
            ["logical-b", "logical-a", "made_from", props],
        ],
    ]
    if legacy_result:
        results = [SimpleNamespace(result_set=rows) for rows in results]
    adapter = Adapter()
    adapter.query = AsyncMock(side_effect=results)
    nodes, edges = await adapter.get_graph_data()

    assert nodes[0] == ("logical-a", {"id": "logical-a"})
    assert edges[0] == (
        "logical-a",
        "logical-b",
        "contains",
        {
            "source_node_id": "logical-a",
            "target_node_id": "logical-b",
        },
    )
    assert edges[1] == (
        "logical-b",
        "logical-a",
        "made from",
        {
            **props,
            "source_node_id": "logical-b",
            "target_node_id": "logical-a",
        },
    )
    assert props["source_node_id"] == "stale-source"  # no input mutation
    query = adapter.query.call_args_list[1].args[0]
    assert "n.id, m.id" in query
    assert "ID(n)" not in query


@pytest.mark.asyncio
async def test_graph_reader_empty_graph():
    adapter = SimpleNamespace(query=AsyncMock(side_effect=[[], []]))
    assert await compat._get_graph_data(adapter) == ([], [])
