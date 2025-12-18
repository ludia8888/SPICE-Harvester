"""
🔥 THINK ULTRA! Type Inference 테스트
다양한 데이터 타입에 대한 자동 추론 테스트
"""

from typing import List

import pytest

from funnel.services.type_inference import FunnelTypeInferenceService
from shared.models.common import DataType


class TestTypeInference:
    """타입 추론 테스트"""

    def test_integer_detection(self):
        """정수 타입 감지 테스트"""
        # 다양한 정수 형식
        test_data = ["1", "42", "100", "999", "1234", "-5", "+10", "0"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "age")

        assert result.inferred_type.type == DataType.INTEGER.value
        assert result.inferred_type.confidence >= 0.9
        assert "integer" in result.inferred_type.reason.lower()
        assert result.total_count == len(test_data)
        assert result.non_empty_count == len(test_data)
        assert result.null_count == 0
        assert result.unique_count == len(test_data)

    def test_decimal_detection(self):
        """소수 타입 감지 테스트"""
        test_data = ["3.14", "2.5", "100.00", "0.1", "-5.5", "1,234.56"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "price")

        assert result.inferred_type.type == DataType.DECIMAL.value
        assert result.inferred_type.confidence >= 0.9
        assert (
            "decimal" in result.inferred_type.reason.lower()
            or "number" in result.inferred_type.reason.lower()
        )
        assert result.inferred_type.metadata is not None
        assert "min" in result.inferred_type.metadata
        assert "max" in result.inferred_type.metadata

    def test_boolean_detection(self):
        """불리언 타입 감지 테스트"""
        # 다양한 불리언 표현
        test_data = ["true", "false", "True", "False", "yes", "no", "Y", "N", "1", "0"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "is_active")

        assert result.inferred_type.type == DataType.BOOLEAN.value
        assert result.inferred_type.confidence >= 0.9
        assert "boolean" in result.inferred_type.reason.lower()

    def test_date_detection_iso_format(self):
        """ISO 날짜 형식 감지 테스트"""
        test_data = ["2023-01-15", "2023-12-31", "2024-02-29", "2022-07-04"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "order_date")

        assert result.inferred_type.type == DataType.DATE.value
        assert result.inferred_type.confidence >= 0.8
        assert "YYYY-MM-DD" in result.inferred_type.reason
        assert result.inferred_type.metadata.get("detected_format") == "YYYY-MM-DD"

    def test_date_detection_us_format(self):
        """미국식 날짜 형식 감지 테스트"""
        test_data = ["01/15/2023", "12/31/2023", "02/29/2024", "07/04/2022"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "date")

        assert result.inferred_type.type == DataType.DATE.value
        assert result.inferred_type.confidence >= 0.8
        assert "MM/DD/YYYY" in result.inferred_type.reason

    def test_date_detection_korean_format(self):
        """한국식 날짜 형식 감지 테스트"""
        test_data = ["2023년 1월 15일", "2023년 12월 31일", "2024년 2월 29일"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "날짜")

        assert result.inferred_type.type == DataType.DATE.value
        assert result.inferred_type.confidence >= 0.8
        assert "YYYY년 MM월 DD일" in result.inferred_type.reason

    def test_datetime_detection(self):
        """날짜시간 타입 감지 테스트"""
        test_data = ["2023-01-15T10:30:00", "2023-12-31T23:59:59", "2024-02-29 14:15:30"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "timestamp")

        assert result.inferred_type.type == DataType.DATETIME.value
        assert result.inferred_type.confidence >= 0.8
        assert "datetime" in result.inferred_type.reason.lower()

    def test_mixed_data_string_fallback(self):
        """혼합 데이터 - 문자열로 폴백"""
        test_data = ["123", "abc", "45.6", "true", "2023-01-01", "some text"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "mixed_data")

        assert result.inferred_type.type == DataType.STRING.value
        assert result.inferred_type.confidence == 1.0
        assert "no specific pattern" in result.inferred_type.reason.lower()

    def test_null_handling(self):
        """Null 값 처리 테스트"""
        test_data = [None, "", "  ", "123", None, "456", "", "789"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "nullable_column")

        assert result.inferred_type.type == DataType.INTEGER.value
        assert result.total_count == len(test_data)
        assert result.non_empty_count == 3
        assert result.null_count == 5  # None, "", "  " 포함
        assert result.unique_count == 3  # "123", "456", "789"

    def test_column_name_hint_email(self):
        """컬럼 이름 힌트 - 이메일"""
        test_data = ["user@example.com", "admin@test.org", "info@company.co.kr"]

        # include_complex_types=True로 테스트
        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "user_email", include_complex_types=True
        )

        # 컬럼 이름이 email을 포함하므로 힌트가 작동해야 함
        assert result.inferred_type.confidence >= 0.7
        if result.inferred_type.type == DataType.EMAIL.value:
            assert "email" in result.inferred_type.reason.lower()

    def test_column_name_hint_phone(self):
        """컬럼 이름 힌트 - 전화번호"""
        test_data = ["010-1234-5678", "02-123-4567", "+82-10-9876-5432"]

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "phone_number", include_complex_types=True
        )

        # 컬럼 이름이 phone을 포함하므로 힌트가 작동해야 함
        assert result.inferred_type.confidence >= 0.7
        if result.inferred_type.type == DataType.PHONE.value:
            assert "phone" in result.inferred_type.reason.lower()

    def test_dataset_analysis(self):
        """전체 데이터셋 분석 테스트"""
        data = [
            ["1", "John", "2023-01-15", "true", "100.50"],
            ["2", "Jane", "2023-01-16", "false", "200.75"],
            ["3", "Bob", "2023-01-17", "true", "150.00"],
        ]
        columns = ["id", "name", "date", "is_active", "amount"]

        results = FunnelTypeInferenceService.analyze_dataset(data, columns)

        assert len(results) == 5

        # 각 컬럼의 타입 확인
        assert results[0].column_name == "id"
        assert results[0].inferred_type.type == DataType.INTEGER.value

        assert results[1].column_name == "name"
        assert results[1].inferred_type.type == DataType.STRING.value

        assert results[2].column_name == "date"
        assert results[2].inferred_type.type == DataType.DATE.value

        assert results[3].column_name == "is_active"
        assert results[3].inferred_type.type == DataType.BOOLEAN.value

        assert results[4].column_name == "amount"
        assert results[4].inferred_type.type == DataType.DECIMAL.value

    def test_large_dataset_sampling(self):
        """대용량 데이터셋 샘플링 테스트"""
        # 1000개 이상의 데이터
        large_data = [[str(i), f"user{i}", "2023-01-01"] for i in range(2000)]
        columns = ["id", "username", "created_date"]

        # sample_size를 100으로 제한
        results = FunnelTypeInferenceService.analyze_dataset(large_data, columns, sample_size=100)

        assert len(results) == 3
        # 샘플링되었어도 타입은 정확히 추론되어야 함
        assert results[0].inferred_type.type == DataType.INTEGER.value
        assert results[1].inferred_type.type == DataType.STRING.value
        assert results[2].inferred_type.type == DataType.DATE.value

    def test_empty_dataset(self):
        """빈 데이터셋 처리"""
        data = []
        columns = ["col1", "col2", "col3"]

        results = FunnelTypeInferenceService.analyze_dataset(data, columns)

        assert len(results) == 3
        # 모든 컬럼이 기본값인 STRING이어야 함
        for result in results:
            assert result.inferred_type.type == DataType.STRING.value
            assert result.inferred_type.confidence == 1.0
            assert "no data available" in result.inferred_type.reason.lower()

    def test_confidence_scores(self):
        """신뢰도 점수 테스트"""
        # 일부 잘못된 데이터가 포함된 경우
        mostly_integers = ["1", "2", "3", "4", "five", "6", "7", "8", "9", "10"]

        result = FunnelTypeInferenceService.infer_column_type(mostly_integers, "mostly_numbers")

        # 90% (9/10)가 정수이므로 정수로 추론되어야 함
        assert result.inferred_type.type == DataType.INTEGER.value
        assert result.inferred_type.confidence >= 0.9
        assert "9/10" in result.inferred_type.reason

    def test_decimal_detection_european_format(self):
        """유럽식 숫자 형식(1.234,56) 감지 테스트"""
        test_data = ["1.234,56", "2.345,00", "10,50"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "amount")

        assert result.inferred_type.type == DataType.DECIMAL.value
        assert result.inferred_type.confidence >= 0.9
        assert result.inferred_type.metadata is not None
        assert result.inferred_type.metadata["min"] == 10.5
        assert result.inferred_type.metadata["max"] == 2345.0

    def test_money_detection_with_symbols(self):
        """통화 기호 기반 money 타입 감지 테스트"""
        test_data = ["$1,234.56", "$10.00", "$99.99"]

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "amount_usd", include_complex_types=True
        )

        assert result.inferred_type.type == DataType.MONEY.value
        assert result.inferred_type.confidence >= 0.9
        assert result.inferred_type.metadata is not None
        assert result.inferred_type.metadata.get("currency") == "USD"
        assert "allowedCurrencies" in result.inferred_type.metadata.get("suggested_constraints", {})

    def test_money_detection_with_asian_currency_formats(self):
        """아시아권 통화 표기(¥/RMB/원) 기반 money 타입 감지 테스트"""
        test_data = ["¥150", "150 RMB", "15,000원"]

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "amount", include_complex_types=True
        )

        assert result.inferred_type.type == DataType.MONEY.value
        assert result.inferred_type.confidence >= 0.9
        assert result.inferred_type.metadata is not None

        currencies = set(result.inferred_type.metadata.get("currencies", []))
        assert currencies == {"CNY", "KRW"}

        suggested = result.inferred_type.metadata.get("suggested_constraints", {})
        assert set(suggested.get("allowedCurrencies", [])) == {"CNY", "KRW"}

    def test_enum_detection_and_constraints(self):
        """열거형(enum) 후보 감지 및 제약조건 제안 테스트"""
        test_data = ["active", "inactive"] * 5  # 10 values, 2 unique

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "status", include_complex_types=True
        )

        assert result.inferred_type.type == DataType.ENUM.value
        assert result.inferred_type.confidence >= 0.7
        assert result.inferred_type.metadata is not None
        suggested = result.inferred_type.metadata.get("suggested_constraints")
        assert isinstance(suggested, dict)
        assert "enum" in suggested

    def test_uuid_detection(self):
        """UUID 타입 감지 테스트"""
        test_data = [
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "00000000-0000-0000-0000-000000000000",
        ]

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "uuid", include_complex_types=True
        )

        assert result.inferred_type.type == "uuid"
        assert result.inferred_type.confidence >= 0.9

    def test_ip_detection(self):
        """IP 주소 타입 감지 테스트"""
        test_data = ["192.168.0.1", "8.8.8.8", "10.0.0.254"]

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "ip_address", include_complex_types=True
        )

        assert result.inferred_type.type == "ip"
        assert result.inferred_type.confidence >= 0.9

    def test_uri_detection(self):
        """URI/URL 타입 감지 테스트"""
        test_data = [
            "https://example.com",
            "http://test.org/path?x=1",
            "https://sub.domain.co.kr/a/b",
        ]

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "website_url", include_complex_types=True
        )

        assert result.inferred_type.type == DataType.URI.value
        assert result.inferred_type.confidence >= 0.9

    def test_json_array_object_detection(self):
        """JSON array/object 타입 감지 테스트"""
        array_data = ['["a","b"]', '["c"]', "[]"]
        object_data = ['{"a":1}', '{"b":2,"c":3}', "{}"]

        array_result = FunnelTypeInferenceService.infer_column_type(
            array_data, "tags", include_complex_types=True
        )
        object_result = FunnelTypeInferenceService.infer_column_type(
            object_data, "payload", include_complex_types=True
        )

        assert array_result.inferred_type.type == DataType.ARRAY.value
        assert array_result.inferred_type.confidence >= 0.9
        assert object_result.inferred_type.type == DataType.OBJECT.value
        assert object_result.inferred_type.confidence >= 0.9

    def test_coordinate_detection(self):
        """좌표(coordinate) 타입 감지 테스트"""
        test_data = ["37.5665,126.9780", "35.1796,129.0756", "-33.8688,151.2093"]

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "coords", include_complex_types=True
        )

        assert result.inferred_type.type == DataType.COORDINATE.value
        assert result.inferred_type.confidence >= 0.9

    def test_phone_suggested_region(self):
        """전화번호 기본 지역 제안(defaultRegion) 테스트"""
        test_data = ["010-1234-5678", "+82-10-9876-5432", "010-0000-0000"]

        result = FunnelTypeInferenceService.infer_column_type(
            test_data, "phone", include_complex_types=True
        )

        assert result.inferred_type.type == DataType.PHONE.value
        assert result.inferred_type.metadata is not None
        suggested = result.inferred_type.metadata.get("suggested_constraints")
        assert isinstance(suggested, dict)
        assert suggested.get("defaultRegion") == "KR"

    def test_ambiguous_date_detection_sets_metadata(self):
        """모호한 날짜(DD/MM vs MM/DD) 감지 시 메타데이터/신뢰도 페널티 테스트"""
        test_data = ["01/02/2023", "03/04/2023", "05/06/2023", "07/08/2023"]

        result = FunnelTypeInferenceService.infer_column_type(test_data, "date")

        assert result.inferred_type.type == DataType.DATE.value
        assert result.inferred_type.metadata is not None
        assert result.inferred_type.metadata.get("ambiguous_count", 0) > 0
        assert result.inferred_type.confidence < 1.0


@pytest.mark.parametrize(
    "test_input,expected_type",
    [
        (["1", "2", "3"], DataType.INTEGER.value),
        (["1.1", "2.2", "3.3"], DataType.DECIMAL.value),
        (["true", "false", "true"], DataType.BOOLEAN.value),
        (["2023-01-01", "2023-01-02"], DataType.DATE.value),
        (["abc", "def", "ghi"], DataType.STRING.value),
    ],
)
def test_parametrized_type_detection(test_input: List[str], expected_type: str):
    """파라미터화된 타입 감지 테스트"""
    result = FunnelTypeInferenceService.infer_column_type(test_input, "test_column")
    assert result.inferred_type.type == expected_type
