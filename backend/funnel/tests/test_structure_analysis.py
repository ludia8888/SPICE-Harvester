"""
🔥 THINK ULTRA! Structure Analysis tests

These tests validate that FunnelStructureAnalyzer can:
- detect data islands (multi-table split)
- detect orientation (transposed tables)
- detect property (key-value) forms
- extract key-values outside tables
"""

from funnel.services.structure_analysis import FunnelStructureAnalyzer
from shared.models.structure_analysis import MergeRange


class TestStructureAnalysis:
    def test_detect_data_island_with_offset_title(self):
        grid = [
            ["매출 보고서", "", "", ""],
            ["", "", "", ""],
            ["작성자:", "홍길동", "", ""],
            ["", "", "", ""],
            ["상품", "수량", "가격", "날짜"],
            ["셔츠", "2", "15,000원", "2024-01-01"],
            ["바지", "1", "20,000원", "2024-01-02"],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=3)
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.mode == "table"
        assert table.bbox.top == 4
        assert table.bbox.left == 0
        assert table.bbox.bottom == 6
        assert table.bbox.right == 3
        assert table.headers == ["상품", "수량", "가격", "날짜"]
        assert len(table.sample_rows) == 2
        assert table.column_provenance is not None
        assert len(table.column_provenance) == 4
        assert table.column_provenance[0].field == "상품"
        assert table.column_provenance[0].data_bbox.top == 5

        # Metadata key-values outside the table should include 작성자 -> 홍길동
        kv = {item.key: item.value for item in result.key_values}
        assert kv.get("작성자") == "홍길동"

    def test_detect_multi_tables_split(self):
        grid = [
            ["", "", "", ""],
            ["id", "name", "price", ""],
            ["1", "A", "$10.00", ""],
            ["2", "B", "$20.00", ""],
            ["", "", "", ""],
            ["메모", "이 아래는 두번째 표", "", ""],
            ["", "", "", ""],
            ["date", "qty", "amount", ""],
            ["2024-01-01", "2", "15,000원", ""],
            ["2024-01-02", "1", "20,000원", ""],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=5)
        assert len(result.tables) == 2
        tops = sorted([t.bbox.top for t in result.tables])
        assert tops == [1, 7]

    def test_split_tables_with_memo_row_no_blank_gap(self):
        """표 사이에 메모 텍스트가 끼어 있어도 테이블을 분리해야 함"""
        grid = [
            ["id", "name", "price", ""],
            ["1", "A", "$10.00", ""],
            ["2", "B", "$20.00", ""],
            ["메모", "이 아래는 두번째 표", "", ""],
            ["date", "qty", "amount", ""],
            ["2024-01-01", "2", "15,000원", ""],
            ["2024-01-02", "1", "20,000원", ""],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=5)
        assert len(result.tables) == 2
        tops = sorted([t.bbox.top for t in result.tables])
        assert tops == [0, 4]

    def test_detect_transposed_table_and_pivot(self):
        grid = [
            ["", "2024-01-01", "2024-01-02"],
            ["매출", "15,000원", "20,000원"],
            ["수량", "2", "1"],
            ["상품", "셔츠", "바지"],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=3)
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.mode == "transposed"
        assert table.headers == ["매출", "수량", "상품"]
        assert len(table.sample_rows) == 2
        assert table.sample_rows[0] == ["15,000원", "2", "셔츠"]

    def test_detect_property_table(self):
        grid = [
            ["공급자", "중국공장A"],
            ["Invoice No", "INV-001"],
            ["총액", "15,000원"],
            ["날짜", "2024-05-01"],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=3)
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.mode == "property"
        assert table.key_values is not None

        kv = {item.key: item.value for item in table.key_values}
        assert kv["공급자"] == "중국공장A"
        assert kv["총액"] == "15,000원"
        assert kv["날짜"] == "2024-05-01"

    def test_hybrid_invoice_property_plus_line_items_no_blank_gap(self):
        """
        하이브리드 문서: 상단은 Key-Value 폼, 하단은 라인아이템 테이블인데
        빈 줄 없이 붙어있는 케이스도 분리해야 함.
        """
        grid = [
            ["공급자", "중국공장A", "", "", ""],
            ["Invoice No", "INV-001", "", "", ""],
            ["날짜", "2024-05-01", "", "", ""],
            ["상품", "수량", "단가", "금액", ""],
            ["셔츠", "2", "¥150", "¥300", ""],
            ["바지", "1", "150 RMB", "150 RMB", ""],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=5)
        assert len(result.tables) == 2
        modes = sorted([t.mode for t in result.tables])
        assert modes == ["property", "table"]

        prop = next(t for t in result.tables if t.mode == "property")
        kv = {item.key: item.value for item in (prop.key_values or [])}
        assert kv.get("공급자") == "중국공장A"
        assert kv.get("Invoice No") == "INV-001"
        assert kv.get("날짜") == "2024-05-01"

        items = next(t for t in result.tables if t.mode == "table")
        assert items.headers[:4] == ["상품", "수량", "단가", "금액"]
        assert len(items.sample_rows) == 2

    def test_merged_cell_flattening_forward_fill(self):
        grid = [
            ["카테고리", "상품", "가격"],
            ["의류", "셔츠", "15,000원"],
            ["", "바지", "20,000원"],
        ]
        merged_cells = [MergeRange(top=1, left=0, bottom=2, right=0)]

        result = FunnelStructureAnalyzer.analyze(
            grid, include_complex_types=True, merged_cells=merged_cells, max_tables=3
        )
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.mode == "table"
        assert table.sample_rows[1][0] == "의류"

    def test_text_only_table_detection(self):
        """숫자/날짜가 거의 없는 텍스트 표도 데이터 섬으로 잡혀야 함"""
        grid = [
            ["상품명", "옵션", "비고"],
            ["셔츠(블루)", "XL", "빠른배송"],
            ["셔츠(블랙)", "L", "예약"],
            ["바지", "M", "교환불가"],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=3)
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.mode == "table"
        assert table.headers == ["상품명", "옵션", "비고"]
        assert len(table.sample_rows) == 3

    def test_text_only_table_detected_even_when_typed_cells_exist_elsewhere(self):
        """타입이 강한 셀이 다른 곳에 있어도, 텍스트-only 표를 놓치지 않아야 함"""
        grid = [
            ["총액", "15,000원", "", ""],
            ["", "", "", ""],
            ["셔츠", "XL", "빠른배송", ""],
            ["바지", "M", "교환불가", ""],
            ["모자", "S", "재고없음", ""],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=5)
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.mode == "table"
        assert table.bbox.top == 2
        assert table.bbox.left == 0
        assert table.headers == ["셔츠", "XL", "빠른배송"]

    def test_multi_header_table(self):
        """2단 헤더(그룹 헤더 + 필드명) 합성 지원"""
        grid = [
            ["상품", "상품", "매출", "매출"],
            ["카테고리", "이름", "수량", "금액"],
            ["의류", "셔츠", "2", "15,000원"],
            ["의류", "바지", "1", "20,000원"],
        ]

        result = FunnelStructureAnalyzer.analyze(grid, include_complex_types=True, max_tables=3)
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.mode == "table"
        assert table.header_rows == 2
        assert table.header_grid is not None
        assert len(table.header_grid) == 2
        assert table.headers[0] == "상품 / 카테고리"
