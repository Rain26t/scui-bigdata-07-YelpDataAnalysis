from __future__ import annotations

from yelp_text_to_sql import ui


def test_build_schema_graph_payload_contains_expected_tables_and_edges() -> None:
    payload = ui._build_schema_graph_payload()

    node_ids = {node["id"] for node in payload["nodes"]}
    edge_pairs = {
        tuple(sorted((edge["source"], edge["target"])))
        for edge in payload["edges"]
    }

    assert {"business", "rating", "users", "checkin"} <= node_ids
    assert ("business", "rating") in edge_pairs
    assert ("business", "checkin") in edge_pairs
    assert ("rating", "users") in edge_pairs
    assert len(edge_pairs) == 3


def test_build_schema_graph_payload_includes_columns_and_checklist() -> None:
    payload = ui._build_schema_graph_payload()
    business_node = next(node for node in payload["nodes"] if node["id"] == "business")

    assert business_node["column_count"] >= 1
    assert any(column["name"] == "business_id" for column in business_node["columns"])
    assert payload["verification_checklist"]
