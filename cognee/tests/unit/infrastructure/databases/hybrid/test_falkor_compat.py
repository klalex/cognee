"""Compatibility patches for older community Falkor adapters."""

import sys
import types
from types import SimpleNamespace

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
    monkeypatch.setitem(
        sys.modules, "cognee_community_hybrid_adapter_falkor.falkor_adapter", child
    )
    monkeypatch.setattr(compat, "_PROVENANCE_KWARGS_PATCHED", False)

    compat.patch_falkor_adapter_provenance_kwargs()

    adapter = LegacyAdapter()
    assert await adapter.add_nodes(["a"], source_ref_key="sr", pipeline_run_id="run") == 1
    assert await adapter.add_edges([("a", "b", "R", {})], source_ref_key="sr") == 1
