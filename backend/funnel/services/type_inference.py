"""
🔥 THINK ULTRA! Funnel Type Inference Service
Automatically detects data types from sample data with confidence scoring
"""

import re
import statistics
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, List, Optional, Dict, Tuple
import logging

# 기존 구현 재사용
from shared.models.common import DataType
from shared.models.type_inference import ColumnAnalysisResult, TypeInferenceResult
from shared.validators.complex_type_validator import ComplexTypeValidator
from shared.validators.money_validator import MoneyValidator

logger = logging.getLogger(__name__)


class PatternBasedTypeDetector:
    """
    🔥 THINK ULTRA! Pattern-Based Type Detection Service

    정규표현식과 패턴 매칭을 사용하여 데이터 타입을 추론합니다.
    
    Pattern-Based Features:
    - 적응형 임계값 시스템 (Adaptive Thresholds)
    - 컨텍스트 기반 타입 추론 (Contextual Analysis)
    - 퍼지 매칭 알고리즘 (Fuzzy Pattern Matching)
    - 다국어 패턴 인식 (Multilingual Pattern Recognition)
    - 복합 타입 탐지 (Composite Type Detection)
    - 통계 분포 분석 (Statistical Distribution Analysis)

    Architecture:
    Data Connector → Pattern Matching Engine → OMS/BFF
    """

    # Date patterns to check
    DATE_PATTERNS = [
        # ISO formats
        (r"^\d{4}-\d{2}-\d{2}$", "%Y-%m-%d", "YYYY-MM-DD"),
        (r"^\d{4}/\d{2}/\d{2}$", "%Y/%m/%d", "YYYY/MM/DD"),
        # US formats
        (r"^\d{2}/\d{2}/\d{4}$", "%m/%d/%Y", "MM/DD/YYYY"),
        (r"^\d{2}-\d{2}-\d{4}$", "%m-%d-%Y", "MM-DD-YYYY"),
        # European formats
        (r"^\d{2}/\d{2}/\d{4}$", "%d/%m/%Y", "DD/MM/YYYY"),
        (r"^\d{2}\.\d{2}\.\d{4}$", "%d.%m.%Y", "DD.MM.YYYY"),
        # Korean format
        (r"^\d{4}년\s*\d{1,2}월\s*\d{1,2}일$", None, "YYYY년 MM월 DD일"),
        # Japanese formats
        (r"^\d{4}年\s*\d{1,2}月\s*\d{1,2}日$", None, "YYYY年MM月DD日"),
        (r"^令和\d+年\s*\d{1,2}月\s*\d{1,2}日$", None, "令和年月日"),
        # Chinese formats
        (r"^\d{4}年\s*\d{1,2}月\s*\d{1,2}日$", None, "YYYY年MM月DD日"),
    ]

    # DateTime patterns
    DATETIME_PATTERNS = [
        # ISO format with time
        (r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}", "%Y-%m-%dT%H:%M:%S", "ISO DateTime"),
        (r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}", "%Y-%m-%d %H:%M:%S", "YYYY-MM-DD HH:MM:SS"),
    ]

    # Boolean values
    BOOLEAN_VALUES = {
        "true": True,
        "false": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
        "1": True,
        "0": False,
        "on": True,
        "off": False,
        "참": True,
        "거짓": False,
        "예": True,
        "아니오": False,
        # Japanese boolean values
        "はい": True,
        "いいえ": False,
        "真": True,
        "偽": False,
        # Chinese boolean values
        "是": True,
        "否": False,
        "真": True,
        "假": False,
    }

    @classmethod
    def infer_column_type(
        cls,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
    ) -> ColumnAnalysisResult:
        """
        🔥 패턴 매칭과 통계 분석으로 컬럼 데이터를 분석하여 타입을 추론합니다.

        Args:
            column_data: 컬럼의 샘플 데이터
            column_name: 컬럼 이름 (타입 힌트용)
            include_complex_types: 복합 타입 검사 여부
            context_columns: 주변 컬럼 데이터 (컨텍스트 분석용)

        Returns:
            ColumnAnalysisResult with pattern-based analysis
        """
        # 통계 정보 수집
        total_count = len(column_data)
        non_empty_values = [v for v in column_data if v is not None and str(v).strip() != ""]
        null_count = len(column_data) - len(non_empty_values)
        non_empty_count = len(non_empty_values)
        unique_values = set(str(v) for v in non_empty_values)
        unique_count = len(unique_values)
        unique_ratio = unique_count / non_empty_count if non_empty_count > 0 else 0.0
        null_ratio = null_count / total_count if total_count > 0 else 0.0

        # 샘플 값 추출 (최대 10개)
        sample_values = list(non_empty_values[:10])

        if not non_empty_values:
            result = TypeInferenceResult(
                type=DataType.STRING.value,
                confidence=1.0,
                reason="All values are empty, defaulting to string type",
            )
            return ColumnAnalysisResult(
                column_name=column_name or "unknown",
                inferred_type=result,
                total_count=total_count,
                non_empty_count=0,
                sample_values=[],
                null_count=null_count,
                unique_count=0,
                null_ratio=null_ratio,
                unique_ratio=0.0,
            )

        # 🔥 Pattern-Based Type Detection
        inference_result = cls._infer_type_advanced(
            non_empty_values, 
            column_name, 
            include_complex_types,
            context_columns,
            sample_size=len(column_data)
        )

        return ColumnAnalysisResult(
            column_name=column_name or "unknown",
            inferred_type=inference_result,
            total_count=total_count,
            non_empty_count=non_empty_count,
            sample_values=sample_values,
            null_count=null_count,
            unique_count=unique_count,
            null_ratio=null_ratio,
            unique_ratio=unique_ratio,
        )

    @classmethod
    def _infer_type_advanced(
        cls,
        values: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
        sample_size: int = 0,
    ) -> TypeInferenceResult:
        """🔥 Pattern-Based Type Detection Engine
        
        Uses adaptive thresholds, contextual analysis, and regex pattern matching.
        """
        # Convert all values to strings for analysis
        str_values = [str(v).strip() for v in values]
        
        # 🔥 Adaptive Thresholds based on sample size and data quality
        adaptive_thresholds = cls._calculate_adaptive_thresholds(str_values, sample_size)
        
        name_hints = cls._get_column_name_hint_scores(column_name)

        # 후보 타입들을 모두 평가한 뒤 점수 기반으로 선택 (순서 편향 제거)
        candidates: List[TypeInferenceResult] = [
            cls._check_boolean_enhanced(str_values, adaptive_thresholds),
            cls._check_integer_enhanced(str_values, adaptive_thresholds),
            cls._check_decimal_enhanced(str_values, adaptive_thresholds),
            cls._check_datetime_enhanced(str_values, adaptive_thresholds),
            cls._check_date_enhanced(str_values, adaptive_thresholds),
        ]

        if include_complex_types:
            candidates.extend(
                cls._check_complex_types_enhanced(
                    str_values,
                    adaptive_thresholds,
                    column_name=column_name,
                    name_hints=name_hints,
                )
            )
            candidates.append(cls._check_enum_enhanced(str_values, adaptive_thresholds, name_hints))

        candidate_summary = cls._summarize_candidates(candidates, adaptive_thresholds, name_hints)
        best = cls._select_best_candidate(candidates, adaptive_thresholds, name_hints)
        if best is not None:
            semantic_label, unit = cls._infer_semantic_label_and_unit(
                values=str_values,
                column_name=column_name,
                inferred=best,
            )
            meta = dict(best.metadata or {})
            if candidate_summary:
                meta["candidate_summary"] = candidate_summary.get("candidates", [])
                gap = candidate_summary.get("confidence_gap")
                if gap is not None:
                    meta["confidence_gap"] = gap
            if semantic_label:
                meta["semantic_label"] = semantic_label
            if unit:
                meta["unit"] = unit
            return TypeInferenceResult(
                type=best.type,
                confidence=best.confidence,
                reason=best.reason,
                metadata=meta or None,
            )

        # Default to string
        fallback = TypeInferenceResult(
            type=DataType.STRING.value,
            confidence=1.0,
            reason="No specific pattern detected, using string type",
        )
        semantic_label, unit = cls._infer_semantic_label_and_unit(
            values=str_values,
            column_name=column_name,
            inferred=fallback,
        )
        meta: Dict[str, Any] = {}
        if candidate_summary:
            meta["candidate_summary"] = candidate_summary.get("candidates", [])
            gap = candidate_summary.get("confidence_gap")
            if gap is not None:
                meta["confidence_gap"] = gap
        if semantic_label:
            meta["semantic_label"] = semantic_label
        if unit:
            meta["unit"] = unit
        if meta:
            fallback.metadata = meta
        return fallback

    @classmethod
    def _get_column_name_hint_scores(cls, column_name: Optional[str]) -> Dict[str, float]:
        """Return type -> hint strength (0.0~1.0) based on column name."""
        if not column_name:
            return {}

        name_lower = column_name.lower()
        hints: Dict[str, float] = {}

        hint_specs: List[Tuple[str, float, List[str]]] = [
            (DataType.EMAIL.value, 0.9, ["email", "e-mail", "mail", "이메일", "メール", "邮箱", "邮件"]),
            (DataType.PHONE.value, 0.85, ["phone", "tel", "mobile", "cell", "전화", "휴대폰", "電話", "手机"]),
            (DataType.URI.value, 0.85, ["url", "uri", "link", "website", "site", "링크", "사이트", "网址", "链接"]),
            (DataType.MONEY.value, 0.8, ["price", "cost", "amount", "fee", "salary", "가격", "금액", "価格"]),
            (DataType.ADDRESS.value, 0.75, ["address", "addr", "주소", "住所", "地址", "dirección", "endereço", "адрес"]),
            (DataType.DATE.value, 0.75, ["date", "day", "날짜", "일자", "日期", "日付"]),
            (DataType.DATETIME.value, 0.75, ["datetime", "timestamp", "time", "시간", "일시", "日時", "时间戳"]),
            (DataType.BOOLEAN.value, 0.7, ["is_", "has_", "flag", "active", "enabled", "여부", "유무"]),
            (DataType.ENUM.value, 0.7, ["status", "state", "type", "category", "kind", "role", "등급", "분류"]),
            ("uuid", 0.85, ["uuid", "guid"]),
            ("ip", 0.85, ["ip", "ip_address", "ipv4", "ipv6"]),
            (DataType.ARRAY.value, 0.7, ["array", "list", "items", "tags"]),
            (DataType.OBJECT.value, 0.7, ["json", "payload", "metadata", "attributes", "object"]),
            (DataType.COORDINATE.value, 0.75, ["coordinate", "coords", "latlng", "좌표", "위도", "경도"]),
        ]

        for type_id, score, keywords in hint_specs:
            if any(keyword in name_lower for keyword in keywords):
                hints[type_id] = max(hints.get(type_id, 0.0), score)

        return hints

    # ---------------------------
    # Semantic label + unit (meaning layer)
    # ---------------------------

    _UNIT_ALIASES: Dict[str, str] = {
        # count/pieces
        "pc": "pcs",
        "pcs": "pcs",
        "ea": "pcs",
        "개": "pcs",
        "건": "pcs",
        "명": "pcs",
        # percent
        "%": "%",
        "percent": "%",
        # weight
        "kg": "kg",
        "g": "g",
        "mg": "mg",
        "lb": "lb",
        "lbs": "lb",
        "oz": "oz",
        # length
        "mm": "mm",
        "cm": "cm",
        "m": "m",
        "km": "km",
        "in": "in",
        "inch": "in",
        "ft": "ft",
        # volume
        "ml": "ml",
        "l": "l",
        "ℓ": "l",
        "cc": "ml",
        # currency-ish (when money type isn't enabled)
        "원": "KRW",
        "₩": "KRW",
        "$": "USD",
        "usd": "USD",
        "eur": "EUR",
        "€": "EUR",
        "gbp": "GBP",
        "£": "GBP",
        "rmb": "CNY",
        "cny": "CNY",
        "jpy": "JPY",
        "¥": "JPY",
        "￥": "JPY",
    }

    @classmethod
    def _extract_unit_from_values(cls, values: List[str]) -> Optional[str]:
        """
        Best-effort unit extraction from sample values.

        Returns a canonical unit id (e.g., 'kg', 'pcs', '%', 'KRW'), or None.
        """
        if not values:
            return None

        counts: Counter[str] = Counter()
        for raw in values[: min(200, len(values))]:
            s = str(raw).strip()
            if not s:
                continue

            # Percent
            if s.endswith("%"):
                counts["%"] += 1
                continue

            # Trailing unit token after a number (e.g., "10 kg", "2pcs", "15,000원")
            if not re.search(r"\d", s):
                continue
            m = re.search(r"([A-Za-z%€£¥￥₩]+|[가-힣]{1,4})\s*$", s)
            if not m:
                continue
            token = m.group(1).strip()
            token_norm = token.lower()
            canon = cls._UNIT_ALIASES.get(token_norm) or cls._UNIT_ALIASES.get(token)  # keep symbols
            if canon:
                counts[canon] += 1

        if not counts:
            return None

        unit, n = counts.most_common(1)[0]
        if n / max(1, sum(counts.values())) < 0.55:
            return None
        return unit

    @classmethod
    def _infer_semantic_label_and_unit(
        cls,
        *,
        values: List[str],
        column_name: Optional[str],
        inferred: TypeInferenceResult,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Derive a semantic label (meaning) + unit from type + hints.

        Output:
        - semantic_label: e.g., PRICE, QTY, SKU, CUSTOMER_NAME, PHONE, EMAIL, DATE, PERCENT ...
        - unit: canonical unit id (e.g., kg, pcs, %, KRW)
        """
        name = (column_name or "").strip().lower()
        unit = cls._extract_unit_from_values(values)

        # Prefer money metadata currency as unit when available
        meta = inferred.metadata or {}
        if inferred.type == DataType.MONEY.value:
            cur = meta.get("currency") or meta.get("defaultCurrency")
            if isinstance(cur, str) and cur:
                unit = cur

        # Basic semantic mapping by inferred type
        if inferred.type == DataType.EMAIL.value:
            return "EMAIL", None
        if inferred.type == DataType.PHONE.value:
            return "PHONE", None
        if inferred.type == DataType.URI.value:
            return "URL", None
        if inferred.type == DataType.DATE.value:
            return "DATE", None
        if inferred.type == DataType.DATETIME.value:
            return "DATETIME", None
        if inferred.type == DataType.BOOLEAN.value:
            return "FLAG", None

        # Money / amount
        if inferred.type == DataType.MONEY.value or (unit in {"KRW", "USD", "EUR", "GBP", "CNY", "JPY"}):
            if any(k in name for k in ["price", "unit_price", "단가", "판매가", "가격"]):
                return "PRICE", unit
            if any(k in name for k in ["amount", "total", "sum", "금액", "총액", "합계", "대금"]):
                return "AMOUNT", unit
            return "AMOUNT", unit

        # Numeric amount/price even when complex money detection isn't enabled
        if inferred.type in {DataType.INTEGER.value, DataType.DECIMAL.value, DataType.FLOAT.value, DataType.DOUBLE.value}:
            if any(k in name for k in ["price", "unit_price", "단가", "판매가", "가격"]):
                return "PRICE", unit
            if any(k in name for k in ["amount", "total", "sum", "금액", "총액", "합계", "대금"]):
                return "AMOUNT", unit

        # Percent
        if unit == "%":
            return "PERCENT", "%"

        # Quantity / count
        if any(k in name for k in ["qty", "quantity", "count", "cnt", "수량", "개수", "数量", "個数"]):
            return "QTY", unit or "pcs"

        # Weight / length / volume by unit
        if unit in {"kg", "g", "mg", "lb", "oz"} or any(k in name for k in ["weight", "무게", "중량", "重量"]):
            return "WEIGHT", unit
        if unit in {"mm", "cm", "m", "km", "in", "ft"} or any(
            k in name for k in ["length", "width", "height", "size", "길이", "가로", "세로", "높이", "サイズ"]
        ):
            return "LENGTH", unit
        if unit in {"ml", "l"} or any(k in name for k in ["volume", "용량", "부피", "容量"]):
            return "VOLUME", unit

        # IDs / codes / SKU
        if any(k in name for k in ["sku", "품번", "품목코드", "상품코드"]):
            return "SKU", None
        if any(k in name for k in ["id", "uuid", "guid", "번호", "no", "code", "코드"]):
            return "ID", None

        # Names / categories / status for strings
        if inferred.type == DataType.STRING.value:
            if any(k in name for k in ["name", "이름", "성명", "상품명", "고객명", "업체명"]):
                return "NAME", None
            if any(k in name for k in ["address", "addr", "주소", "住所", "地址"]):
                return "ADDRESS", None
            if any(k in name for k in ["status", "state", "상태", "구분", "状態"]):
                return "STATUS", None
            if any(k in name for k in ["category", "cat", "카테고리", "분류", "分類"]):
                return "CATEGORY", None

        return None, unit

    @classmethod
    def _min_confidence_for_type(
        cls, type_id: str, thresholds: Dict[str, float], name_hints: Dict[str, float]
    ) -> float:
        """Minimum acceptance confidence for a type (name hints can lower it)."""
        if type_id == DataType.BOOLEAN.value:
            base = thresholds["boolean"]
        elif type_id == DataType.INTEGER.value:
            base = thresholds["integer"]
        elif type_id == DataType.DECIMAL.value:
            base = thresholds["decimal"]
        elif type_id == DataType.DATE.value:
            base = thresholds["date"]
        elif type_id == DataType.DATETIME.value:
            base = thresholds["datetime"]
        elif type_id == DataType.ENUM.value:
            base = thresholds["enum"]
        else:
            base = thresholds["complex"]

        # Column name strongly suggests this type -> allow lower evidence threshold
        if type_id in name_hints:
            base = min(base, 0.70)

        return base

    @classmethod
    def _type_priority(cls, type_id: str) -> int:
        """Tie-break priority (lower is preferred)."""
        priorities = {
            DataType.EMAIL.value: 0,
            DataType.URI.value: 1,
            "uuid": 2,
            "ip": 3,
            DataType.PHONE.value: 4,
            DataType.MONEY.value: 5,
            DataType.ARRAY.value: 6,
            DataType.OBJECT.value: 7,
            DataType.COORDINATE.value: 8,
            DataType.BOOLEAN.value: 10,
            DataType.INTEGER.value: 11,
            DataType.DECIMAL.value: 12,
            DataType.DATETIME.value: 13,
            DataType.DATE.value: 14,
            DataType.ENUM.value: 20,
        }
        return priorities.get(type_id, 100)

    @classmethod
    def _select_best_candidate(
        cls,
        candidates: List[TypeInferenceResult],
        thresholds: Dict[str, float],
        name_hints: Dict[str, float],
    ) -> Optional[TypeInferenceResult]:
        # If the column name strongly implies a specialized type, prefer it when it validates reasonably.
        strong_types = {
            DataType.EMAIL.value,
            DataType.PHONE.value,
            DataType.URI.value,
            DataType.MONEY.value,
            DataType.ADDRESS.value,
            DataType.COORDINATE.value,
            "uuid",
            "ip",
        }
        strong_hints = sorted(
            (t for t, s in name_hints.items() if s >= 0.85 and t in strong_types),
            key=lambda t: name_hints.get(t, 0.0),
            reverse=True,
        )
        for hinted_type in strong_hints:
            hinted_candidates = [c for c in candidates if c.type == hinted_type]
            if not hinted_candidates:
                continue
            best_hinted = max(hinted_candidates, key=lambda c: c.confidence)
            if best_hinted.confidence >= cls._min_confidence_for_type(
                best_hinted.type, thresholds, name_hints
            ):
                return best_hinted

        eligible: List[Tuple[float, int, TypeInferenceResult]] = []
        for cand in candidates:
            min_conf = cls._min_confidence_for_type(cand.type, thresholds, name_hints)
            if cand.confidence < min_conf:
                continue

            # Small tie-break boost if column name hints the same type
            hint_boost = 0.02 if cand.type in name_hints else 0.0
            rank_score = min(1.0, cand.confidence + hint_boost)
            eligible.append((rank_score, -cls._type_priority(cand.type), cand))

        if not eligible:
            return None

        eligible.sort(reverse=True, key=lambda t: (t[0], t[1]))
        return eligible[0][2]

    @classmethod
    def _summarize_candidates(
        cls,
        candidates: List[TypeInferenceResult],
        thresholds: Dict[str, float],
        name_hints: Dict[str, float],
    ) -> Dict[str, Any]:
        if not candidates:
            return {}

        sorted_candidates = sorted(
            candidates,
            key=lambda cand: (cand.confidence, -cls._type_priority(cand.type)),
            reverse=True,
        )
        summary = []
        for cand in sorted_candidates[:3]:
            min_conf = cls._min_confidence_for_type(cand.type, thresholds, name_hints)
            summary.append(
                {
                    "type": cand.type,
                    "confidence": cand.confidence,
                    "meets_threshold": cand.confidence >= min_conf,
                }
            )
        gap = None
        if len(sorted_candidates) > 1:
            gap = max(0.0, sorted_candidates[0].confidence - sorted_candidates[1].confidence)

        return {"candidates": summary, "confidence_gap": gap}

    @classmethod
    def _check_complex_types_enhanced(
        cls,
        values: List[str],
        thresholds: Dict[str, float],
        column_name: Optional[str],
        name_hints: Dict[str, float],
    ) -> List[TypeInferenceResult]:
        """Evaluate complex/specialized types via validators and heuristics."""
        candidates: List[TypeInferenceResult] = []

        # Phone (heuristic) – also provides region hint metadata
        candidates.append(cls._check_phone_enhanced(values, thresholds, column_name))

        # Validator-based candidates (high precision)
        candidates.append(cls._check_validator_type(values, DataType.EMAIL.value))
        candidates.append(cls._check_validator_type(values, DataType.URI.value))
        candidates.append(cls._check_validator_type(values, DataType.MONEY.value))
        candidates.append(cls._check_validator_type(values, "uuid"))
        candidates.append(cls._check_validator_type(values, "ip"))
        candidates.append(cls._check_validator_type(values, DataType.ARRAY.value))
        candidates.append(cls._check_validator_type(values, DataType.OBJECT.value))
        candidates.append(cls._check_validator_type(values, DataType.COORDINATE.value))

        # Column-name-hinted validation for types that are easy to overfit
        for hinted_type in [
            DataType.EMAIL.value,
            DataType.PHONE.value,
            DataType.URI.value,
            DataType.MONEY.value,
            DataType.ADDRESS.value,
            "uuid",
            "ip",
        ]:
            if hinted_type not in name_hints:
                continue
            candidates.append(
                cls._check_validator_type(
                    values,
                    hinted_type,
                    sample_limit=min(50, len(values)),
                    hint_reason=f"Column name suggests {hinted_type}",
                )
            )

        return candidates

    @classmethod
    def _check_validator_type(
        cls,
        values: List[str],
        type_id: str,
        sample_limit: int = 50,
        hint_reason: Optional[str] = None,
        constraints: Optional[Dict[str, Any]] = None,
    ) -> TypeInferenceResult:
        """Check values against ComplexTypeValidator for a given type."""
        if not values:
            return TypeInferenceResult(type=type_id, confidence=0.0, reason="No values to validate")

        sample = values[: min(sample_limit, len(values))]
        effective_constraints: Dict[str, Any] = dict(constraints or {})
        if type_id == DataType.MONEY.value:
            derived = cls._derive_money_constraints_from_samples(sample)
            if derived.get("allowedCurrencies") and "allowedCurrencies" not in effective_constraints:
                effective_constraints["allowedCurrencies"] = derived["allowedCurrencies"]

        valid_count = 0
        money_currencies: Counter[str] = Counter()
        money_amounts: List[float] = []
        ambiguous_yen_samples = 0
        ambiguous_yen_resolved: Counter[str] = Counter()
        for v in sample:
            if type_id == DataType.MONEY.value and isinstance(v, str):
                if any(sym in v for sym in MoneyValidator.AMBIGUOUS_SYMBOLS):
                    ambiguous_yen_samples += 1

            is_valid, _, normalized = ComplexTypeValidator.validate(v, type_id, effective_constraints)
            if is_valid:
                valid_count += 1
                if type_id == DataType.MONEY.value and isinstance(normalized, dict):
                    currency = normalized.get("currency")
                    amount = normalized.get("amount")
                    if isinstance(currency, str):
                        money_currencies[currency] += 1
                        if isinstance(v, str) and any(sym in v for sym in MoneyValidator.AMBIGUOUS_SYMBOLS):
                            ambiguous_yen_resolved[currency] += 1
                    try:
                        if amount is not None:
                            money_amounts.append(float(amount))
                    except (TypeError, ValueError):
                        pass

        confidence = valid_count / len(sample) if sample else 0.0
        reason_prefix = f"{hint_reason}. " if hint_reason else ""
        metadata: Dict[str, Any] = {"matched": valid_count, "total": len(sample)}

        if type_id == DataType.MONEY.value and valid_count > 0 and money_currencies:
            currencies = [c for c, _ in money_currencies.most_common()]
            default_currency = money_currencies.most_common(1)[0][0]
            metadata.update(
                {
                    "currencies": currencies,
                    "currency": default_currency,
                    "min": min(money_amounts) if money_amounts else None,
                    "max": max(money_amounts) if money_amounts else None,
                    "suggested_constraints": {
                        "allowedCurrencies": currencies,
                        "defaultCurrency": default_currency,
                    },
                }
            )
            if ambiguous_yen_samples > 0:
                yen_context = set(effective_constraints.get("allowedCurrencies", [])) & {"CNY", "JPY"}
                metadata.update(
                    {
                        "ambiguous_symbol_samples": ambiguous_yen_samples,
                        "ambiguous_symbol_resolved": {
                            c: n for c, n in ambiguous_yen_resolved.most_common()
                        },
                        "ambiguous_symbol_candidates": ["CNY", "JPY"] if len(yen_context) != 1 else [],
                    }
                )

        return TypeInferenceResult(
            type=type_id,
            confidence=confidence,
            reason=f"{reason_prefix}{valid_count}/{len(sample)} samples validate as {type_id}",
            metadata=metadata,
        )

    @classmethod
    def _derive_money_constraints_from_samples(cls, values: List[str]) -> Dict[str, Any]:
        """
        Derive money constraints (allowedCurrencies) from explicit tokens in samples.

        Purpose: disambiguate symbols like ¥/￥ without hard-coding a region-specific default.
        """
        allowed: set[str] = set()
        for raw in values:
            s = str(raw).strip()
            if not s:
                continue

            # Unit words / localized aliases
            for word, code in MoneyValidator.CURRENCY_WORD_ALIASES.items():
                if word in s:
                    allowed.add(code)

            # Unambiguous currency symbols
            for sym, code in MoneyValidator.CURRENCY_SYMBOLS.items():
                if sym in s:
                    allowed.add(code)

            # Currency codes (ISO-ish), including aliases like RMB/CNH
            for code in re.findall(r"\b[A-Za-z]{3}\b", s):
                norm = MoneyValidator.CURRENCY_CODE_ALIASES.get(code.upper(), code.upper())
                if norm in MoneyValidator.COMMON_CURRENCIES:
                    allowed.add(norm)

        return {"allowedCurrencies": sorted(allowed)} if allowed else {}

    @classmethod
    def _check_enum_enhanced(
        cls, values: List[str], thresholds: Dict[str, float], name_hints: Dict[str, float]
    ) -> TypeInferenceResult:
        """Detect enum-like categorical strings and propose constraints."""
        if not values:
            return TypeInferenceResult(type=DataType.ENUM.value, confidence=0.0, reason="No values")

        counts = Counter(values)
        unique_count = len(counts)
        total = len(values)
        unique_ratio = unique_count / total if total > 0 else 0.0

        # Heuristic: small unique set and repeated values => enum candidate
        max_unique_allowed = min(50, max(2, int(total * 0.2)))
        is_enum_like = unique_count <= max_unique_allowed and unique_ratio <= 0.2

        confidence = max(0.0, min(1.0, 1.0 - unique_ratio))
        allowed_values = [v for v, _ in counts.most_common(min(unique_count, 50))]

        if not is_enum_like:
            return TypeInferenceResult(
                type=DataType.ENUM.value,
                confidence=confidence,
                reason=f"Enum heuristic not met (unique_ratio={unique_ratio:.2f}, unique_count={unique_count})",
                metadata={"unique_count": unique_count, "total": total},
            )

        return TypeInferenceResult(
            type=DataType.ENUM.value,
            confidence=confidence,
            reason=f"Enum-like distribution: {unique_count} unique / {total} samples (unique_ratio={unique_ratio:.2f})",
            metadata={
                "allowed_values": allowed_values,
                "suggested_constraints": {"enum": allowed_values},
                "unique_count": unique_count,
                "total": total,
            },
        )

    @classmethod
    def _check_boolean(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are boolean"""
        total = len(values)
        matched = 0

        for value in values:
            if value.lower() in cls.BOOLEAN_VALUES:
                matched += 1

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.9:
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) match boolean patterns"
            return TypeInferenceResult(
                type=DataType.BOOLEAN.value, confidence=confidence, reason=reason
            )

        return TypeInferenceResult(
            type=DataType.BOOLEAN.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values match boolean patterns",
        )

    @classmethod
    def _check_integer(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are integers"""
        total = len(values)
        matched = 0

        for value in values:
            # Remove thousand separators
            cleaned = value.replace(",", "").replace(" ", "")
            try:
                # Check if it's an integer (not float)
                int_val = int(cleaned)
                # Verify it's not a float disguised as int
                if (
                    cleaned == str(int_val)
                    or cleaned.startswith("+")
                    and cleaned[1:] == str(int_val)
                ):
                    matched += 1
            except ValueError:
                # Skip non-integer values, this is expected for type inference
                pass

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.9:
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid integers"
            return TypeInferenceResult(
                type=DataType.INTEGER.value, confidence=confidence, reason=reason
            )

        return TypeInferenceResult(
            type=DataType.INTEGER.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values are valid integers",
        )

    @classmethod
    def _check_decimal(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are decimal numbers"""
        total = len(values)
        matched = 0
        has_decimals = 0

        for value in values:
            # Handle both . and , as decimal separators
            cleaned = value.replace(" ", "")

            # Try comma as decimal separator (European style)
            if "," in cleaned and "." not in cleaned:
                cleaned = cleaned.replace(",", ".")
            # Try period as decimal separator (US style)
            elif "," in cleaned and "." in cleaned:
                # Assume comma is thousand separator
                cleaned = cleaned.replace(",", "")

            try:
                Decimal(cleaned)
                matched += 1
                if "." in cleaned:
                    has_decimals += 1
            except (ValueError, InvalidOperation):
                # Skip non-decimal values, this is expected for type inference
                pass

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.9:
            if has_decimals > 0:
                reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid numbers, {has_decimals} with decimals"
            else:
                # All numbers but no decimals - could be integer
                reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid numbers (no decimals found)"
                confidence *= 0.8  # Reduce confidence since integers would be more appropriate
            return TypeInferenceResult(
                type=DataType.DECIMAL.value, confidence=confidence, reason=reason
            )

        return TypeInferenceResult(
            type=DataType.DECIMAL.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values are valid numbers",
        )

    @classmethod
    def _check_date(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are dates"""
        total = len(values)
        matched = 0
        pattern_counts = Counter()

        for value in values:
            for pattern_regex, format_str, pattern_name in cls.DATE_PATTERNS:
                if re.match(pattern_regex, value):
                    if format_str:
                        try:
                            datetime.strptime(value, format_str)
                            matched += 1
                            pattern_counts[pattern_name] += 1
                            break
                        except ValueError:
                            continue
                    else:
                        # Korean date format
                        matched += 1
                        pattern_counts[pattern_name] += 1
                        break

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.8 and pattern_counts:
            most_common_pattern = pattern_counts.most_common(1)[0][0]
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) match date pattern {most_common_pattern}"
            metadata = {"detected_format": most_common_pattern}
            return TypeInferenceResult(
                type=DataType.DATE.value, confidence=confidence, reason=reason, metadata=metadata
            )

        return TypeInferenceResult(
            type=DataType.DATE.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values match date patterns",
        )

    @classmethod
    def _check_datetime(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are datetime"""
        total = len(values)
        matched = 0
        pattern_counts = Counter()

        for value in values:
            for pattern_regex, format_str, pattern_name in cls.DATETIME_PATTERNS:
                if re.match(pattern_regex, value):
                    try:
                        # Handle timezone info
                        test_value = value.replace("Z", "+00:00")
                        if format_str:
                            # Extract just the datetime part without timezone
                            datetime_part = test_value.split("+")[0].split("Z")[0]
                            datetime.strptime(datetime_part, format_str)
                        matched += 1
                        pattern_counts[pattern_name] += 1
                        break
                    except ValueError:
                        continue

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.8 and pattern_counts:
            most_common_pattern = pattern_counts.most_common(1)[0][0]
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) match datetime pattern {most_common_pattern}"
            metadata = {"detected_format": most_common_pattern}
            return TypeInferenceResult(
                type=DataType.DATETIME.value,
                confidence=confidence,
                reason=reason,
                metadata=metadata,
            )

        return TypeInferenceResult(
            type=DataType.DATETIME.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values match datetime patterns",
        )

    @classmethod
    def _check_column_name_hints(cls, column_name: str) -> Optional[TypeInferenceResult]:
        """Check column name for type hints"""
        name_lower = column_name.lower()

        # Email hints
        email_keywords = ["email", "e-mail", "mail", "이메일", "メール"]
        for keyword in email_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.EMAIL.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests email type",
                )

        # Phone hints
        phone_keywords = ["phone", "tel", "mobile", "cell", "전화", "휴대폰", "電話"]
        for keyword in phone_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.PHONE.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests phone type",
                )

        # URL hints
        url_keywords = ["url", "link", "website", "site", "링크", "사이트"]
        for keyword in url_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.URI.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests URL type",
                )

        # Money hints
        money_keywords = ["price", "cost", "amount", "fee", "salary", "가격", "금액", "価格"]
        for keyword in money_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.MONEY.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests money type",
                )

        # Address hints
        address_keywords = ["address", "addr", "주소", "住所"]
        for keyword in address_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.ADDRESS.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests address type",
                )

        # Coordinate hints
        coord_keywords = [
            "coordinate",
            "lat",
            "lng",
            "longitude",
            "latitude",
            "좌표",
            "위도",
            "경도",
        ]
        for keyword in coord_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.COORDINATE.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests coordinate type",
                )

        return None

    @classmethod
    def _calculate_adaptive_thresholds(cls, values: List[str], sample_size: int) -> Dict[str, float]:
        """🔥 Adaptive Thresholds: tune acceptance based on sample size.

        Notes:
        - 작은 샘플은 우연/오탐 위험이 크므로 더 보수적으로(임계값 ↑)
        - 큰 샘플은 소량의 노이즈를 허용(임계값 ↓)
        """
        base_thresholds = {
            "boolean": 0.95,
            "integer": 0.90,
            "decimal": 0.90,
            "date": 0.90,
            "datetime": 0.90,
            # complex / enum are handled in selection stage, but keep defaults here
            "complex": 0.90,
            "enum": 0.95,
        }

        if sample_size <= 10:
            delta = 0.05
        elif sample_size >= 1000:
            delta = -0.05
        else:
            delta = 0.0

        thresholds: Dict[str, float] = {}
        for key, base in base_thresholds.items():
            thresholds[key] = max(0.70, min(0.99, base + delta))

        return thresholds
    
    @classmethod
    def _analyze_context(cls, column_name: str, context_columns: Dict[str, List[Any]]) -> Dict[str, Any]:
        """🔥 Contextual Analysis: Analyze surrounding columns for type hints"""
        context_hints = {
            'related_types': [],
            'pattern_consistency': 0.0,
            'composite_type_hints': []
        }
        
        if not column_name or not context_columns:
            return context_hints
            
        # Look for related columns (name + email pattern, etc.)
        name_lower = column_name.lower()
        
        for other_col, other_data in context_columns.items():
            other_lower = other_col.lower()
            
            # Detect composite patterns
            if 'name' in name_lower and 'email' in other_lower:
                context_hints['composite_type_hints'].append('person_identity')
            elif 'first' in name_lower and 'last' in other_lower:
                context_hints['composite_type_hints'].append('full_name')
            elif 'lat' in name_lower and 'lng' in other_lower:
                context_hints['composite_type_hints'].append('coordinates')
            elif 'street' in name_lower and ('city' in other_lower or 'zip' in other_lower):
                context_hints['composite_type_hints'].append('address')
                
        return context_hints
    
    @classmethod
    def _check_column_name_hints_enhanced(cls, column_name: str) -> Optional[TypeInferenceResult]:
        """🔥 Enhanced Column Name Hints with Multilingual Support"""
        name_lower = column_name.lower()
        
        # Enhanced Email hints (multilingual)
        email_keywords = [
            "email", "e-mail", "mail", "メール", "이메일", "邮箱", "邮件",
            "correo", "correio", "почта", "ایمیل"
        ]
        for keyword in email_keywords:
            if keyword in name_lower:
                confidence = 0.85 if keyword in ['email', 'mail'] else 0.75
                return TypeInferenceResult(
                    type=DataType.EMAIL.value,
                    confidence=confidence,
                    reason=f"Enhanced multilingual column name analysis: '{column_name}' suggests email type",
                )
        
        # Enhanced Phone hints (multilingual)
        phone_keywords = [
            "phone", "tel", "mobile", "cell", "전화", "휴대폰", "電話", "手机",
            "telefono", "telefone", "телефон", "تلفن"
        ]
        for keyword in phone_keywords:
            if keyword in name_lower:
                confidence = 0.8
                return TypeInferenceResult(
                    type=DataType.PHONE.value,
                    confidence=confidence,
                    reason=f"Enhanced multilingual column name analysis: '{column_name}' suggests phone type",
                )
        
        # Enhanced Address hints
        address_keywords = [
            "address", "addr", "주소", "住所", "地址", "dirección", "endereço", "адрес"
        ]
        for keyword in address_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.ADDRESS.value,
                    confidence=0.75,
                    reason=f"Enhanced multilingual column name analysis: '{column_name}' suggests address type",
                )
        
        return None
    
    @classmethod
    def _check_boolean_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced Boolean Detection with Fuzzy Matching"""
        total = len(values)
        matched = 0
        
        for value in values:
            value_lower = value.lower().strip()
            # Trim common punctuation without allowing substring matches (avoid "true story" -> boolean)
            token = re.sub(r"^[\\W_]+|[\\W_]+$", "", value_lower)
            
            # Exact match
            if token in cls.BOOLEAN_VALUES:
                matched += 1
                
        exact_confidence = matched / total if total > 0 else 0
        fuzzy_confidence = exact_confidence
        
        # Use higher confidence with explanation
        if exact_confidence >= thresholds['boolean']:
            return TypeInferenceResult(
                type=DataType.BOOLEAN.value,
                confidence=exact_confidence,
                reason=f"Enhanced boolean detection: {matched}/{total} exact matches ({exact_confidence*100:.1f}%)",
            )
        
        return TypeInferenceResult(
            type=DataType.BOOLEAN.value,
            confidence=fuzzy_confidence,
            reason=f"Enhanced boolean analysis: insufficient matches ({fuzzy_confidence*100:.1f}%)",
        )
    
    @classmethod
    def _check_integer_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced Integer Detection with Statistical Analysis"""
        total = len(values)
        matched = 0
        int_values = []
        
        for value in values:
            raw = value.strip()
            if not raw:
                continue

            # Allow proper thousands separators (comma/space/underscore) and leading zeros
            is_valid_int = False
            normalized = raw

            if re.fullmatch(r"[+-]?\d+", raw):
                is_valid_int = True
                normalized = raw
            elif "," in raw and re.fullmatch(r"[+-]?\d{1,3}(,\d{3})+", raw):
                is_valid_int = True
                normalized = raw.replace(",", "")
            elif "_" in raw and re.fullmatch(r"[+-]?\d{1,3}(_\d{3})+", raw):
                is_valid_int = True
                normalized = raw.replace("_", "")
            elif " " in raw and re.fullmatch(r"[+-]?\d{1,3}( \d{3})+", raw):
                is_valid_int = True
                normalized = raw.replace(" ", "")

            if not is_valid_int:
                continue

            try:
                int_val = int(normalized)
                matched += 1
                int_values.append(int_val)
            except ValueError:
                continue
                
        confidence = matched / total if total > 0 else 0
        
        # Statistical analysis for better confidence
        if int_values:
            # Check for patterns that suggest integers
            value_range = max(int_values) - min(int_values) if len(int_values) > 1 else 0
            avg_digits = statistics.mean([len(str(abs(v))) for v in int_values])
            
            # Boost confidence for typical integer patterns
            if avg_digits <= 3 and value_range < 1000:  # IDs, counts, etc.
                confidence = min(1.0, confidence * 1.1)
            elif all(v >= 0 for v in int_values):  # All positive
                confidence = min(1.0, confidence * 1.05)
                
        if confidence >= thresholds['integer']:
            stats_info = (
                f", range: {max(int_values) - min(int_values)}, "
                f"avg_digits: {statistics.mean([len(str(abs(v))) for v in int_values]):.1f}"
                if int_values
                else ""
            )
            metadata = None
            if int_values:
                metadata = {
                    "min": min(int_values),
                    "max": max(int_values),
                    "mean": statistics.mean(int_values),
                    "std": statistics.stdev(int_values) if len(int_values) > 1 else 0.0,
                    "matched": matched,
                    "total": total,
                }
            return TypeInferenceResult(
                type=DataType.INTEGER.value,
                confidence=confidence,
                reason=f"Enhanced integer analysis: {matched}/{total} values ({confidence*100:.1f}%){stats_info}",
                metadata=metadata,
            )
        
        return TypeInferenceResult(
            type=DataType.INTEGER.value,
            confidence=confidence,
            reason=f"Enhanced integer analysis: insufficient matches ({confidence*100:.1f}%)",
        )
    
    @classmethod
    def _check_decimal_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced Decimal Detection with Distribution Analysis"""
        total = len(values)
        matched = 0
        has_decimals = 0
        decimal_values = []
        
        for value in values:
            raw = value.strip().replace(" ", "")
            if not raw:
                continue

            # Allow parentheses for negatives: (123.45)
            sign = ""
            if raw.startswith("(") and raw.endswith(")"):
                sign = "-"
                raw = raw[1:-1].strip()

            # Extract sign
            if raw.startswith(("+", "-")):
                sign = raw[0]
                raw = raw[1:]

            normalized = raw
            has_fraction = False

            if "," in raw and "." in raw:
                # Decide decimal separator by the last occurrence
                if raw.rfind(",") > raw.rfind("."):
                    # 1.234,56 -> 1234.56
                    normalized = raw.replace(".", "").replace(",", ".")
                    has_fraction = True
                else:
                    # 1,234.56 -> 1234.56
                    normalized = raw.replace(",", "")
                    has_fraction = True
            elif "," in raw:
                parts = raw.split(",")
                if len(parts) == 2 and 1 <= len(parts[1]) <= 6 and len(parts[1]) != 3:
                    # 12,34 -> 12.34 (comma as decimal)
                    normalized = raw.replace(",", ".")
                    has_fraction = True
                elif len(parts) >= 2 and all(len(p) == 3 for p in parts[1:]):
                    # 1,234,567 -> 1234567 (comma as thousands)
                    normalized = raw.replace(",", "")
                    has_fraction = False
                else:
                    # Ambiguous: try decimal first
                    if len(parts) == 2 and 1 <= len(parts[1]) <= 6:
                        normalized = raw.replace(",", ".")
                        has_fraction = True
                    else:
                        continue
            elif "." in raw:
                dot_count = raw.count(".")
                if dot_count > 1:
                    # 1.234.567 -> 1234567 (dot as thousands)
                    normalized = raw.replace(".", "")
                    has_fraction = False
                else:
                    # Single dot: treat as decimal separator
                    normalized = raw
                    has_fraction = True

            try:
                decimal_val = float(Decimal(f"{sign}{normalized}"))
                matched += 1
                decimal_values.append(decimal_val)
                if has_fraction:
                    has_decimals += 1
            except (ValueError, InvalidOperation):
                pass
                
        confidence = matched / total if total > 0 else 0
        
        # Statistical distribution analysis
        if decimal_values and len(decimal_values) > 2:
            try:
                std_dev = statistics.stdev(decimal_values)
                mean_val = statistics.mean(decimal_values)
                coefficient_of_variation = std_dev / abs(mean_val) if mean_val != 0 else float('inf')
                
                # Adjust confidence based on distribution characteristics
                if coefficient_of_variation < 0.5:  # Low variability suggests structured data
                    confidence = min(1.0, confidence * 1.1)
                    
            except statistics.StatisticsError:
                pass
                
        if confidence >= thresholds['decimal']:
            decimal_info = f", {has_decimals} with decimal places" if has_decimals > 0 else " (no decimal places found)"
            stats_info = f", std_dev: {statistics.stdev(decimal_values):.2f}" if len(decimal_values) > 2 else ""
            
            # Reduce confidence if no actual decimals but claiming decimal type
            if has_decimals == 0:
                confidence *= 0.8
            metadata = None
            if decimal_values:
                metadata = {
                    "min": min(decimal_values),
                    "max": max(decimal_values),
                    "mean": statistics.mean(decimal_values),
                    "std": statistics.stdev(decimal_values) if len(decimal_values) > 1 else 0.0,
                    "matched": matched,
                    "total": total,
                    "has_decimals": has_decimals,
                }
                
            return TypeInferenceResult(
                type=DataType.DECIMAL.value,
                confidence=confidence,
                reason=f"Enhanced decimal analysis: {matched}/{total} values ({confidence*100:.1f}%){decimal_info}{stats_info}",
                metadata=metadata,
            )
            
        return TypeInferenceResult(
            type=DataType.DECIMAL.value,
            confidence=confidence,
            reason=f"Enhanced decimal analysis: insufficient matches ({confidence*100:.1f}%)",
        )
    
    @classmethod
    def _check_date_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced Date Detection with strict parsing and ambiguity handling."""
        total = len(values)
        matched = 0
        ambiguous = 0
        pattern_counts: Counter[str] = Counter()

        def _try_parse(value: str, fmt: str) -> bool:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                return False

        for value in values:
            v = value.strip()
            if not v:
                continue

            # ISO-ish: YYYY-MM-DD / YYYY/M/D
            if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", v) and _try_parse(v, "%Y-%m-%d"):
                matched += 1
                pattern_counts["YYYY-MM-DD"] += 1
                continue
            if re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", v) and _try_parse(v, "%Y/%m/%d"):
                matched += 1
                pattern_counts["YYYY/MM/DD"] += 1
                continue

            # Ambiguous: 01/02/2024 or 01-02-2024
            m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", v)
            if m:
                a, b, _year = m.groups()
                month_first = _try_parse(v, "%m/%d/%Y")
                day_first = _try_parse(v, "%d/%m/%Y")
                if month_first and not day_first:
                    matched += 1
                    pattern_counts["MM/DD/YYYY"] += 1
                    continue
                if day_first and not month_first:
                    matched += 1
                    pattern_counts["DD/MM/YYYY"] += 1
                    continue
                if month_first and day_first:
                    a_i, b_i = int(a), int(b)
                    if a_i > 12:
                        matched += 1
                        pattern_counts["DD/MM/YYYY"] += 1
                        continue
                    if b_i > 12:
                        matched += 1
                        pattern_counts["MM/DD/YYYY"] += 1
                        continue
                    # Truly ambiguous (both <= 12)
                    ambiguous += 1
                    matched += 1
                    pattern_counts["MM/DD/YYYY"] += 1
                    continue

            m = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", v)
            if m:
                a, b, _year = m.groups()
                month_first = _try_parse(v, "%m-%d-%Y")
                day_first = _try_parse(v, "%d-%m-%Y")
                if month_first and not day_first:
                    matched += 1
                    pattern_counts["MM-DD-YYYY"] += 1
                    continue
                if day_first and not month_first:
                    matched += 1
                    pattern_counts["DD-MM-YYYY"] += 1
                    continue
                if month_first and day_first:
                    a_i, b_i = int(a), int(b)
                    if a_i > 12:
                        matched += 1
                        pattern_counts["DD-MM-YYYY"] += 1
                        continue
                    if b_i > 12:
                        matched += 1
                        pattern_counts["MM-DD-YYYY"] += 1
                        continue
                    ambiguous += 1
                    matched += 1
                    pattern_counts["MM-DD-YYYY"] += 1
                    continue

            # Dot format: DD.MM.YYYY
            if re.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{4}", v) and _try_parse(v, "%d.%m.%Y"):
                matched += 1
                pattern_counts["DD.MM.YYYY"] += 1
                continue

            # Korean: YYYY년 M월 D일
            m = re.fullmatch(r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일", v)
            if m:
                y, mo, d = map(int, m.groups())
                try:
                    datetime(y, mo, d)
                    matched += 1
                    pattern_counts["YYYY년 MM월 DD일"] += 1
                    continue
                except ValueError:
                    pass

            # Japanese/Chinese: YYYY年M月D日
            m = re.fullmatch(r"(\d{4})[年]\s*(\d{1,2})[月]\s*(\d{1,2})[日]", v)
            if m:
                y, mo, d = map(int, m.groups())
                try:
                    datetime(y, mo, d)
                    matched += 1
                    pattern_counts["YYYY年MM月DD日"] += 1
                    continue
                except ValueError:
                    pass

            # Reiwa era (keep as date-like, but not convertible here)
            if re.fullmatch(r"令和\d+年\s*\d{1,2}月\s*\d{1,2}日", v):
                matched += 1
                pattern_counts["令和年月日"] += 1
                continue

        confidence = matched / total if total > 0 else 0.0
        if confidence >= thresholds["date"] and pattern_counts:
            most_common = pattern_counts.most_common(1)[0][0]
            final_confidence = confidence
            if ambiguous > 0:
                final_confidence = max(0.0, confidence - 0.05)
            return TypeInferenceResult(
                type=DataType.DATE.value,
                confidence=final_confidence,
                reason=f"Enhanced date detection: {matched}/{total} matches ({final_confidence*100:.1f}%), primary pattern: {most_common}",
                metadata={
                    "detected_format": most_common,
                    "ambiguous_count": ambiguous,
                    "matched": matched,
                    "total": total,
                },
            )

        return TypeInferenceResult(
            type=DataType.DATE.value,
            confidence=confidence,
            reason=f"Enhanced date analysis: insufficient matches ({confidence*100:.1f}%)",
        )
    
    @classmethod
    def _check_datetime_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced DateTime Detection with Advanced Parsing"""
        total = len(values)
        matched = 0
        fuzzy_matched = 0
        pattern_counts = Counter()
        timezone_detected = 0
        
        for value in values:
            exact_match = False
            
            # Check for timezone indicators
            if any(tz in value.upper() for tz in ['UTC', 'GMT', '+00:00', 'Z', 'EST', 'PST']):
                timezone_detected += 1
                
            # Exact pattern matching
            for pattern_regex, format_str, pattern_name in cls.DATETIME_PATTERNS:
                if re.match(pattern_regex, value):
                    try:
                        test_value = value.replace("Z", "+00:00")
                        if format_str:
                            datetime_part = test_value.split("+")[0].split("Z")[0]
                            datetime.strptime(datetime_part, format_str)
                        matched += 1
                        pattern_counts[pattern_name] += 1
                        exact_match = True
                        break
                    except ValueError:
                        continue
                        
            # Fuzzy matching for datetime-like patterns
            if not exact_match:
                if re.search(r'\d{4}-\d{2}-\d{2}', value) and re.search(r'\d{2}:\d{2}', value):
                    fuzzy_matched += 1
                elif 'T' in value and ':' in value:  # ISO-like format
                    fuzzy_matched += 1
                    
        exact_confidence = matched / total if total > 0 else 0
        fuzzy_confidence = (matched + fuzzy_matched * 0.7) / total if total > 0 else 0
        
        # Boost confidence if timezones are detected
        if timezone_detected > 0:
            timezone_boost = min(0.1, timezone_detected / total * 0.2)
            fuzzy_confidence = min(1.0, fuzzy_confidence + timezone_boost)
            
        if exact_confidence >= thresholds['datetime'] and pattern_counts:
            most_common_pattern = pattern_counts.most_common(1)[0][0]
            tz_info = f", {timezone_detected} with timezone info" if timezone_detected > 0 else ""
            return TypeInferenceResult(
                type=DataType.DATETIME.value,
                confidence=exact_confidence,
                reason=f"Enhanced datetime detection: {matched}/{total} exact matches ({exact_confidence*100:.1f}%), pattern: {most_common_pattern}{tz_info}",
                metadata={"detected_format": most_common_pattern, "timezone_count": timezone_detected}
            )
        elif fuzzy_confidence >= thresholds['datetime'] * 0.75:
            tz_info = f", {timezone_detected} with timezone info" if timezone_detected > 0 else ""
            return TypeInferenceResult(
                type=DataType.DATETIME.value,
                confidence=fuzzy_confidence,
                reason=f"Enhanced fuzzy datetime detection: {matched} exact + {fuzzy_matched} fuzzy matches ({fuzzy_confidence*100:.1f}%){tz_info}",
                metadata={"fuzzy_matches": fuzzy_matched, "timezone_count": timezone_detected}
            )
            
        return TypeInferenceResult(
            type=DataType.DATETIME.value,
            confidence=fuzzy_confidence,
            reason=f"Enhanced datetime analysis: insufficient matches ({fuzzy_confidence*100:.1f}%)",
        )

    @classmethod
    def analyze_dataset(
        cls,
        data: List[List[Any]],
        columns: List[str],
        sample_size: Optional[int] = 1000,
        include_complex_types: bool = False,
    ) -> List[ColumnAnalysisResult]:
        """
        전체 데이터셋을 분석하여 각 컬럼의 타입을 추론합니다.

        Args:
            data: 데이터셋 (행 리스트)
            columns: 컬럼 이름 리스트
            sample_size: 분석할 샘플 크기
            include_complex_types: 복합 타입 검사 여부

        Returns:
            각 컬럼의 분석 결과 리스트
        """
        results = []

        # 샘플 크기 제한
        if sample_size and len(data) > sample_size:
            data = data[:sample_size]

        # 데이터가 없는 경우
        if not data:
            for col in columns:
                result = TypeInferenceResult(
                    type=DataType.STRING.value,
                    confidence=1.0,
                    reason="No data available, defaulting to string type",
                )
                results.append(
                    ColumnAnalysisResult(
                        column_name=col,
                        inferred_type=result,
                        sample_values=[],
                        null_count=0,
                        unique_count=0,
                    )
                )
            return results

        # 각 컬럼 분석
        # 🔥 Build context for advanced analysis
        context_columns = {}
        for i, col_name in enumerate(columns):
            context_columns[col_name] = [row[i] if i < len(row) else None for row in data]
        
        for i, column_name in enumerate(columns):
            column_data = [row[i] if i < len(row) else None for row in data]
            
            # Create context without current column
            current_context = {k: v for k, v in context_columns.items() if k != column_name}
            
            analysis_result = cls.infer_column_type(
                column_data, 
                column_name, 
                include_complex_types,
                current_context
            )
            results.append(analysis_result)

        return results

    @classmethod
    def _check_phone_enhanced(cls, values: List[str], thresholds: Dict[str, float], column_name: Optional[str] = None) -> TypeInferenceResult:
        """🔥 Enhanced Phone Number Detection with Global Patterns"""
        total = len(values)
        matched = 0
        fuzzy_matched = 0
        kr_like = 0
        us_like = 0
        
        # Enhanced phone patterns (ordered from most specific to most generic)
        kr_patterns = [
            r"^\d{3}-\d{4}-\d{4}$",  # Korean format
            r"^\d{3}[-\s]\d{4}[-\s]\d{4}$",  # Korean alt
        ]
        us_pattern = r"^\+?1?[-\s]?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{4}$"
        plus_digits_pattern = r"^\+\d{10,15}$"
        digits_only_pattern = r"^\d{10,15}$"
        intl_pattern = r"^\+?\d{1,4}[-\s]?\(?\d{1,4}\)?[-\s]?\d{3,4}[-\s]?\d{4}$"
        
        for value in values:
            # Clean the value
            cleaned = value.strip()
            
            # Exact pattern matching (prefer specific patterns first)
            exact_match = False
            if any(re.match(p, cleaned) for p in kr_patterns):
                matched += 1
                exact_match = True
                kr_like += 1
            elif re.match(us_pattern, cleaned):
                matched += 1
                exact_match = True
                us_like += 1
            elif re.match(plus_digits_pattern, cleaned):
                matched += 1
                exact_match = True
                if cleaned.startswith("+82"):
                    kr_like += 1
                elif cleaned.startswith("+1"):
                    us_like += 1
            elif re.match(digits_only_pattern, cleaned):
                matched += 1
                exact_match = True
                if cleaned.startswith("010"):
                    kr_like += 1
            elif re.match(intl_pattern, cleaned):
                matched += 1
                exact_match = True
                if cleaned.startswith("82") or cleaned.startswith("+82"):
                    kr_like += 1
                elif cleaned.startswith("1") or cleaned.startswith("+1"):
                    us_like += 1
                    
            # Fuzzy matching if no exact match
            if not exact_match:
                # Remove all non-digit characters and check length
                digits_only = re.sub(r'[^\d]', '', cleaned)
                if 7 <= len(digits_only) <= 15:  # Reasonable phone number length
                    # Check if it has phone-like separators
                    if any(sep in cleaned for sep in ['-', ' ', '.', '(', ')']):
                        fuzzy_matched += 1
                    # Check if it starts with country code patterns
                    elif cleaned.startswith(('+', '00')) or (len(digits_only) >= 10):
                        fuzzy_matched += 1
                if cleaned.startswith("+82") or cleaned.startswith("82") or cleaned.startswith("010"):
                    kr_like += 1
                if cleaned.startswith("+1") or cleaned.startswith("1"):
                    us_like += 1
                        
        exact_confidence = matched / total if total > 0 else 0
        fuzzy_confidence = (matched + fuzzy_matched * 0.8) / total if total > 0 else 0

        suggested_constraints = None
        if kr_like > 0 and kr_like >= us_like:
            suggested_constraints = {"defaultRegion": "KR"}
        elif us_like > 0 and us_like > kr_like:
            suggested_constraints = {"defaultRegion": "US"}
        
        # Boost confidence if column name suggests phone
        column_boost = 0
        if column_name:
            phone_keywords = ["phone", "tel", "mobile", "cell", "전화", "휴대폰", "電話", "手机"]
            if any(keyword in column_name.lower() for keyword in phone_keywords):
                column_boost = 0.1
                
        final_confidence = min(1.0, fuzzy_confidence + column_boost)
        metadata = {
            "matched": matched,
            "total": total,
            "suggested_constraints": suggested_constraints,
        }
        
        if exact_confidence >= 0.8:
            return TypeInferenceResult(
                type=DataType.PHONE.value,
                confidence=exact_confidence,
                reason=f"Enhanced phone detection: {matched}/{total} exact pattern matches ({exact_confidence*100:.1f}%)",
                metadata=metadata,
            )
        elif final_confidence >= 0.7:
            boost_info = f" + column hint boost" if column_boost > 0 else ""
            return TypeInferenceResult(
                type=DataType.PHONE.value,
                confidence=final_confidence,
                reason=f"Enhanced fuzzy phone detection: {matched} exact + {fuzzy_matched} fuzzy matches ({final_confidence*100:.1f}%){boost_info}",
                metadata=metadata,
            )
            
        return TypeInferenceResult(
            type=DataType.PHONE.value,
            confidence=final_confidence,
            reason=f"Enhanced phone analysis: insufficient matches ({final_confidence*100:.1f}%)",
            metadata=metadata,
        )


# Compatibility alias for backward compatibility
FunnelTypeInferenceService = PatternBasedTypeDetector
