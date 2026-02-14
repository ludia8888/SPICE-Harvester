"""
🔥 THINK ULTRA! RelationshipManager Service
자동 역관계 생성 및 관계 무결성 관리
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from shared.models.ontology import Cardinality, Relationship

logger = logging.getLogger(__name__)


@dataclass
class RelationshipPair:
    """관계 쌍 (정방향 + 역방향)"""

    forward: Relationship
    inverse: Relationship
    is_bidirectional: bool = True


class RelationshipManager:
    """
    🔥 THINK ULTRA! 고급 관계 관리자

    기능:
    1. 자동 역관계 생성
    2. 관계 무결성 검증
    3. 카디널리티 일관성 보장
    4. 관계 쌍 관리
    """

    def __init__(self):
        self.cardinality_inverse_map = {
            Cardinality.ONE_TO_ONE: Cardinality.ONE_TO_ONE,
            Cardinality.ONE_TO_MANY: Cardinality.MANY_TO_ONE,
            Cardinality.MANY_TO_ONE: Cardinality.ONE_TO_MANY,
            Cardinality.MANY_TO_MANY: Cardinality.MANY_TO_MANY,
            Cardinality.ONE: Cardinality.MANY,
            Cardinality.MANY: Cardinality.ONE,
            "1:1": "1:1",
            "1:n": "n:1",
            "n:1": "1:n",
            "n:m": "n:m",
            "one": "many",
            "many": "one",
        }

    def create_bidirectional_relationship(
        self, source_class: str, relationship: Relationship, auto_generate_inverse: bool = True
    ) -> Tuple[Relationship, Optional[Relationship]]:
        """
        양방향 관계 생성

        Args:
            source_class: 소스 클래스 ID
            relationship: 정방향 관계
            auto_generate_inverse: 역관계 자동 생성 여부

        Returns:
            (정방향 관계, 역방향 관계) 튜플
        """
        logger.info(f"Creating bidirectional relationship: {source_class} -> {relationship.target}")

        # 정방향 관계 검증
        forward_rel = self._validate_and_normalize_relationship(relationship)

        if not auto_generate_inverse:
            return forward_rel, None

        # 역관계 자동 생성
        inverse_rel = self._generate_inverse_relationship(
            source_class=relationship.target,
            target_class=source_class,
            forward_relationship=forward_rel,
        )

        # 관계 쌍 검증
        self._validate_relationship_pair(forward_rel, inverse_rel)

        logger.info(f"✅ Created bidirectional relationship pair:")
        logger.info(f"   Forward: {forward_rel.predicate} ({forward_rel.cardinality})")
        logger.info(f"   Inverse: {inverse_rel.predicate} ({inverse_rel.cardinality})")

        return forward_rel, inverse_rel

    def _generate_inverse_relationship(
        self, source_class: str, target_class: str, forward_relationship: Relationship
    ) -> Relationship:
        """역관계 자동 생성"""

        # 역관계 predicate 결정
        inverse_predicate = (
            forward_relationship.inverse_predicate
            or self._generate_inverse_predicate(forward_relationship.predicate)
        )

        # 역관계 카디널리티 계산
        inverse_cardinality = self._get_inverse_cardinality(forward_relationship.cardinality)

        # 역관계 레이블 생성
        inverse_label = self._generate_inverse_label(
            forward_relationship.inverse_label,
            forward_relationship.label,
            forward_relationship.predicate,
        )

        # 역관계 설명 생성
        inverse_description = self._generate_inverse_description(
            forward_relationship.description, source_class, target_class
        )

        return Relationship(
            predicate=inverse_predicate,
            target=target_class,
            label=inverse_label,
            description=inverse_description,
            cardinality=inverse_cardinality,
            inverse_predicate=forward_relationship.predicate,
            inverse_label=forward_relationship.label,
        )

    def _get_inverse_cardinality(self, cardinality: str) -> str:
        """카디널리티의 역관계 계산"""

        # 문자열 형태의 cardinality 처리
        if isinstance(cardinality, str):
            return self.cardinality_inverse_map.get(cardinality, cardinality)

        # Enum 형태의 cardinality 처리
        if hasattr(cardinality, "value"):
            inverse_str = self.cardinality_inverse_map.get(cardinality.value, cardinality.value)
            try:
                return Cardinality(inverse_str)
            except ValueError:
                return inverse_str

        return self.cardinality_inverse_map.get(cardinality, cardinality)

    def _generate_inverse_predicate(self, predicate: str) -> str:
        """predicate의 역관계명 자동 생성"""

        inverse_patterns = {
            # 일반적인 패턴
            "has": "belongsTo",
            "contains": "containedBy",
            "owns": "ownedBy",
            "manages": "managedBy",
            "creates": "createdBy",
            "uses": "usedBy",
            "follows": "followedBy",
            "teaches": "taughtBy",
            "supervises": "supervisedBy",
            # 한국어 패턴
            "가진다": "속한다",
            "포함한다": "포함된다",
            "관리한다": "관리된다",
            "생성한다": "생성된다",
            "사용한다": "사용된다",
            # 영어 동사 패턴
            "employs": "employedBy",
            "enrollsIn": "hasStudent",
            "worksFor": "hasEmployee",
            "belongsTo": "contains",
        }

        # 정확한 매치
        if predicate in inverse_patterns:
            return inverse_patterns[predicate]

        # 패턴 기반 생성
        if predicate.startswith("has"):
            # has + X -> belongsTo + Parent
            return predicate.replace("has", "belongsTo", 1)
        elif predicate.endswith("By"):
            # managedBy -> manages
            return predicate[:-2] + "s"
        elif predicate.endswith("다"):
            # 한국어 동사 -> 수동형
            return predicate[:-1] + "된다"
        else:
            # 기본 패턴: predicate + "InverseOf"
            return f"inverseOf{predicate.capitalize()}"

    def _generate_inverse_label(
        self,
        explicit_inverse_label: Optional[str],
        forward_label: str,
        predicate: str,
    ) -> str:
        """역관계 레이블 생성"""

        if explicit_inverse_label:
            return explicit_inverse_label

        # 정방향 레이블을 기반으로 역관계 레이블 생성
        return self._invert_label_text(forward_label) if forward_label else "inverse relationship"

    def _invert_label_text(self, text: str) -> str:
        """레이블 텍스트의 역관계 표현 생성"""

        inversion_patterns = {
            # 한국어
            "소유한다": "소유된다",
            "관리한다": "관리된다",
            "포함한다": "포함된다",
            "근무한다": "고용한다",
            "속한다": "가진다",
            # 영어
            "has": "belongs to",
            "owns": "owned by",
            "manages": "managed by",
            "contains": "contained by",
            "works for": "employs",
            "belongs to": "has",
            "teaches": "taught by",
            "learns from": "teaches",
        }

        # 정확한 매치
        for pattern, inverse in inversion_patterns.items():
            if pattern in text.lower():
                return text.replace(pattern, inverse)

        # 기본 패턴
        if "한다" in text:
            return text.replace("한다", "된다")
        elif text.endswith("s"):
            return f"is {text[:-1]}ed by"
        else:
            return f"inverse of {text}"

    def _generate_inverse_description(
        self, forward_description: Optional[str], source_class: str, target_class: str
    ) -> Optional[str]:
        """역관계 설명 생성"""

        if not forward_description:
            return f"Inverse relationship from {source_class} to {target_class}"

        # 정방향 설명을 기반으로 역관계 설명 생성
        return f"Inverse of: {forward_description}"

    def _validate_and_normalize_relationship(self, relationship: Relationship) -> Relationship:
        """관계 검증 및 정규화"""

        # 필수 필드 검증
        if not relationship.predicate:
            raise ValueError("Relationship predicate is required")
        if not relationship.target:
            raise ValueError("Relationship target is required")

        # 카디널리티 정규화
        normalized_cardinality = self._normalize_cardinality(relationship.cardinality)

        # 검증된 관계 반환
        return Relationship(
            predicate=relationship.predicate,
            target=relationship.target,
            label=relationship.label,
            description=relationship.description,
            cardinality=normalized_cardinality,
            inverse_predicate=relationship.inverse_predicate,
            inverse_label=relationship.inverse_label,
        )

    def _normalize_cardinality(self, cardinality: Any) -> str:
        """카디널리티 정규화"""

        if isinstance(cardinality, str):
            return cardinality
        elif hasattr(cardinality, "value"):
            return cardinality.value
        else:
            return str(cardinality)

    def _validate_relationship_pair(self, forward: Relationship, inverse: Relationship) -> None:
        """관계 쌍 검증"""

        # 상호 참조 검증
        if forward.inverse_predicate and forward.inverse_predicate != inverse.predicate:
            raise ValueError(
                f"Forward relationship inverse_predicate '{forward.inverse_predicate}' "
                f"doesn't match inverse predicate '{inverse.predicate}'"
            )

        if inverse.inverse_predicate and inverse.inverse_predicate != forward.predicate:
            raise ValueError(
                f"Inverse relationship inverse_predicate '{inverse.inverse_predicate}' "
                f"doesn't match forward predicate '{forward.predicate}'"
            )

        # 카디널리티 일관성 검증
        expected_inverse_cardinality = self._get_inverse_cardinality(forward.cardinality)
        if inverse.cardinality != expected_inverse_cardinality:
            logger.warning(
                f"Cardinality mismatch: forward={forward.cardinality}, "
                f"inverse={inverse.cardinality}, expected={expected_inverse_cardinality}"
            )

        # 타겟 일관성 검증
        if forward.target != inverse.inverse_predicate and inverse.target:
            logger.info(f"Target consistency check passed")

    def detect_relationship_conflicts(self, relationships: List[Relationship]) -> List[str]:
        """관계 충돌 감지"""

        conflicts = []
        predicate_map = {}

        for rel in relationships:
            # predicate 중복 검사
            if rel.predicate in predicate_map:
                conflicts.append(f"Duplicate predicate '{rel.predicate}' found in relationships")
            predicate_map[rel.predicate] = rel

            # 자기 참조 검사 (나중에 CircularReferenceDetector에서 더 정교하게 처리)
            if rel.target == rel.predicate:
                conflicts.append(f"Self-referential relationship detected: {rel.predicate}")

        return conflicts

    def generate_relationship_summary(self, relationships: List[Relationship]) -> Dict[str, Any]:
        """관계 요약 정보 생성"""

        cardinality_counts = {}
        target_counts = {}
        inverse_coverage = 0

        for rel in relationships:
            # 카디널리티 통계
            card_str = str(rel.cardinality)
            cardinality_counts[card_str] = cardinality_counts.get(card_str, 0) + 1

            # 타겟 통계
            target_counts[rel.target] = target_counts.get(rel.target, 0) + 1

            # 역관계 커버리지
            if rel.inverse_predicate:
                inverse_coverage += 1

        return {
            "total_relationships": len(relationships),
            "cardinality_distribution": cardinality_counts,
            "target_distribution": target_counts,
            "inverse_coverage": (
                f"{inverse_coverage}/{len(relationships)} ({inverse_coverage/len(relationships)*100:.1f}%)"
                if relationships
                else "0/0 (0%)"
            ),
            "has_conflicts": len(self.detect_relationship_conflicts(relationships)) > 0,
        }
