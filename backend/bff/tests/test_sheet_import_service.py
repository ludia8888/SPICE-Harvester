from bff.services.sheet_import_service import FieldMapping, SheetImportService


def test_coerce_integer_with_currency_suffix():
    result = SheetImportService.build_instances(
        columns=["amount"],
        rows=[["15,000원"]],
        mappings=[FieldMapping(source_field="amount", target_field="qty")],
        target_field_types={"qty": "xsd:integer"},
    )
    assert result["errors"] == []
    assert result["instances"] == [{"qty": 15000}]


def test_coerce_decimal_with_currency_symbol():
    result = SheetImportService.build_instances(
        columns=["price"],
        rows=[["¥150"]],
        mappings=[FieldMapping(source_field="price", target_field="unit_price")],
        target_field_types={"unit_price": "xsd:decimal"},
    )
    assert result["errors"] == []
    assert result["instances"][0]["unit_price"] == 150


def test_coerce_date_accepts_common_separators():
    result = SheetImportService.build_instances(
        columns=["order_date"],
        rows=[["2024/05/01"]],
        mappings=[FieldMapping(source_field="order_date", target_field="d")],
        target_field_types={"d": "xsd:date"},
    )
    assert result["errors"] == []
    assert result["instances"] == [{"d": "2024-05-01"}]


def test_boolean_parsing():
    result = SheetImportService.build_instances(
        columns=["active"],
        rows=[["Y"], ["0"], ["false"]],
        mappings=[FieldMapping(source_field="active", target_field="is_active")],
        target_field_types={"is_active": "xsd:boolean"},
    )
    assert result["errors"] == []
    assert result["instances"] == [{"is_active": True}, {"is_active": False}, {"is_active": False}]


def test_error_rows_are_reported_and_can_be_filtered():
    result = SheetImportService.build_instances(
        columns=["a", "b"],
        rows=[["1", "x"]],
        mappings=[
            FieldMapping(source_field="a", target_field="good_int"),
            FieldMapping(source_field="b", target_field="bad_int"),
        ],
        target_field_types={"good_int": "xsd:integer", "bad_int": "xsd:integer"},
    )
    assert result["error_row_indices"] == [0]
    assert len(result["errors"]) == 1
    assert result["instances"] == [{"good_int": 1}]
