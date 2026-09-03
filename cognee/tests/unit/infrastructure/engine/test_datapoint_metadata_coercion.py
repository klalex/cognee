import json
from types import SimpleNamespace

from cognee.infrastructure.engine import DataPoint
from cognee.modules.migrations.versions.namespace_entity_type_node_ids import _make_node


def test_get_embeddable_property_names_parses_json_metadata_string():
    node = SimpleNamespace(
        metadata=json.dumps({"index_fields": ["name"]}),
        name="Alice",
    )

    assert DataPoint.get_embeddable_property_names(node) == ["name"]
    assert DataPoint.get_embeddable_data(node) == "Alice"
    assert DataPoint.get_embeddable_properties(node) == ["Alice"]


def test_make_node_carrier_deserializes_metadata_for_migration_roundtrip():
    carrier = _make_node(
        {
            "id": "new-id",
            "type": "Entity",
            "name": "Alice",
            "metadata": json.dumps({"index_fields": ["name"]}),
        }
    )

    assert DataPoint.get_embeddable_property_names(carrier) == ["name"]
