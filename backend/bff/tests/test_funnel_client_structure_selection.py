from bff.services.funnel_client import FunnelClient


class TestFunnelClientStructureSelection:
    def test_select_primary_table_prefers_record_table_over_property(self):
        structure = {
            "metadata": {"sheet_title": "Test", "worksheet_title": "Sheet1"},
            "tables": [
                {
                    "id": "t_prop",
                    "mode": "property",
                    "confidence": 0.99,
                    "bbox": {"top": 0, "left": 0, "bottom": 3, "right": 1},
                    "headers": [],
                    "sample_rows": [],
                    "key_values": [{"key": "A", "value": "1"}],
                },
                {
                    "id": "t_table",
                    "mode": "table",
                    "confidence": 0.80,
                    "bbox": {"top": 10, "left": 0, "bottom": 20, "right": 5},
                    "header_rows": 1,
                    "headers": ["col1", "col2"],
                    "sample_rows": [["1", "2"]],
                    "inferred_schema": [],
                },
            ],
        }

        chosen = FunnelClient._select_primary_table(structure)
        assert chosen["id"] == "t_table"

    def test_select_primary_table_prefers_higher_confidence(self):
        structure = {
            "metadata": {"sheet_title": "Test", "worksheet_title": "Sheet1"},
            "tables": [
                {
                    "id": "t1",
                    "mode": "table",
                    "confidence": 0.70,
                    "bbox": {"top": 0, "left": 0, "bottom": 10, "right": 3},
                    "header_rows": 1,
                    "headers": ["a"],
                    "sample_rows": [["1"]],
                    "inferred_schema": [],
                },
                {
                    "id": "t2",
                    "mode": "table",
                    "confidence": 0.90,
                    "bbox": {"top": 0, "left": 0, "bottom": 3, "right": 10},
                    "header_rows": 1,
                    "headers": ["a", "b"],
                    "sample_rows": [["1", "2"]],
                    "inferred_schema": [],
                },
            ],
        }

        chosen = FunnelClient._select_primary_table(structure)
        assert chosen["id"] == "t2"

    def test_structure_table_to_preview_estimates_total_rows(self):
        structure = {
            "metadata": {"sheet_id": "sid", "sheet_title": "Test", "worksheet_title": "Sheet1"},
            "tables": [],
        }
        table = {
            "id": "t1",
            "mode": "table",
            "confidence": 0.9,
            "bbox": {"top": 5, "left": 0, "bottom": 11, "right": 3},  # height=7
            "header_rows": 2,
            "headers": ["a", "b", "c", "d"],
            "sample_rows": [["1", "2", "3", "4"]],
            "inferred_schema": [],
        }

        preview = FunnelClient._structure_table_to_preview(
            structure=structure,
            table=table,
            sheet_url="https://docs.google.com/spreadsheets/d/sid/edit",
            worksheet_name="Sheet1",
        )

        assert preview["columns"] == ["a", "b", "c", "d"]
        assert preview["preview_rows"] == 1
        assert preview["total_rows"] == 5

    def test_select_requested_table_by_id(self):
        structure = {
            "tables": [
                {"id": "table_1", "mode": "table", "confidence": 0.6, "bbox": {"top": 0, "left": 0, "bottom": 3, "right": 2}},
                {"id": "table_2", "mode": "table", "confidence": 0.9, "bbox": {"top": 10, "left": 0, "bottom": 12, "right": 4}},
            ]
        }

        chosen = FunnelClient._select_requested_table(structure, table_id="table_2", table_bbox=None)
        assert chosen["id"] == "table_2"

    def test_select_requested_table_by_bbox(self):
        structure = {
            "tables": [
                {"id": "table_1", "mode": "table", "confidence": 0.6, "bbox": {"top": 0, "left": 0, "bottom": 3, "right": 2}},
                {"id": "table_2", "mode": "table", "confidence": 0.9, "bbox": {"top": 10, "left": 0, "bottom": 12, "right": 4}},
            ]
        }

        chosen = FunnelClient._select_requested_table(
            structure,
            table_id=None,
            table_bbox={"top": 0, "left": 0, "bottom": 3, "right": 2},
        )
        assert chosen["id"] == "table_1"

    def test_select_requested_table_unknown_id_raises(self):
        structure = {"tables": [{"id": "table_1", "bbox": {"top": 0, "left": 0, "bottom": 1, "right": 1}}]}

        try:
            FunnelClient._select_requested_table(structure, table_id="missing", table_bbox=None)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "Unknown table_id" in str(e)
