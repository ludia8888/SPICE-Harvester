"""
🔥 THINK ULTRA! RelationshipPathTracker
관계 경로 추적 및 네비게이션 시스템
"""

import heapq
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from shared.models.ontology import OntologyResponse, Relationship
from oms.utils.cardinality_utils import inverse_cardinality

logger = logging.getLogger(__name__)


class PathType(Enum):
    """경로 유형"""

    SHORTEST = "shortest"  # 최단 경로
    ALL_PATHS = "all"  # 모든 경로
    WEIGHTED = "weighted"  # 가중치 고려 경로
    SEMANTIC = "semantic"  # 의미적 관련성 경로


class TraversalDirection(Enum):
    """탐색 방향"""

    FORWARD = "forward"  # 정방향만
    BACKWARD = "backward"  # 역방향만
    BIDIRECTIONAL = "both"  # 양방향


@dataclass
class RelationshipHop:
    """관계 홉 (한 단계 관계)"""

    source: str
    target: str
    predicate: str
    cardinality: str
    is_inverse: bool = False
    weight: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RelationshipPath:
    """관계 경로"""

    start_entity: str
    end_entity: str
    hops: List[RelationshipHop]
    total_weight: float
    path_type: PathType
    length: int
    semantic_score: float = 0.0
    confidence: float = 1.0

    @property
    def entities(self) -> List[str]:
        """경로상의 모든 엔티티 반환"""
        if not self.hops:
            return [self.start_entity]

        entities = [self.start_entity]
        for hop in self.hops:
            entities.append(hop.target)
        return entities

    @property
    def predicates(self) -> List[str]:
        """경로상의 모든 predicate 반환"""
        return [hop.predicate for hop in self.hops]

    def to_readable_string(self) -> str:
        """읽기 쉬운 경로 문자열 반환"""
        if not self.hops:
            return self.start_entity

        path_str = self.start_entity
        for hop in self.hops:
            direction = "←" if hop.is_inverse else "→"
            path_str += f" {direction}[{hop.predicate}] {hop.target}"

        return path_str


@dataclass
class PathQuery:
    """경로 탐색 쿼리"""

    start_entity: str
    end_entity: Optional[str] = None
    max_depth: int = 5
    path_type: PathType = PathType.SHORTEST
    direction: TraversalDirection = TraversalDirection.BIDIRECTIONAL
    include_predicates: Optional[Set[str]] = None
    exclude_predicates: Optional[Set[str]] = None
    include_cardinalities: Optional[Set[str]] = None
    weight_predicates: Optional[Dict[str, float]] = None
    semantic_boost: Optional[Dict[str, float]] = None


class RelationshipPathTracker:
    """
    🔥 THINK ULTRA! 관계 경로 추적기

    기능:
    1. 최단 경로 탐색 (Dijkstra)
    2. 모든 경로 탐색 (DFS)
    3. 가중치 기반 최적 경로
    4. 의미적 관련성 기반 경로
    5. 관계 네비게이션 유틸리티
    6. 경로 분석 및 통계
    """

    def __init__(self):
        self.relationship_graph: Dict[str, List[RelationshipHop]] = defaultdict(list)
        self.reverse_graph: Dict[str, List[RelationshipHop]] = defaultdict(list)
        self.entities: Set[str] = set()

        # 기본 가중치 (predicate별)
        self.default_weights = {
            # 강한 관계 (낮은 가중치)
            "hasParent": 0.5,
            "parentOf": 0.5,
            "hasChild": 0.5,
            "childOf": 0.5,
            "partOf": 0.3,
            "contains": 0.3,
            "instanceOf": 0.2,
            "hasInstance": 0.2,
            # 중간 관계
            "worksFor": 1.0,
            "hasEmployee": 1.0,
            "belongsTo": 1.0,
            "has": 1.0,
            "manages": 1.2,
            "managedBy": 1.2,
            # 약한 관계 (높은 가중치)
            "related": 2.0,
            "associated": 2.0,
            "similar": 2.5,
            "mentions": 3.0,
        }

        # 카디널리티별 가중치 조정
        self.cardinality_weights = {
            "1:1": 0.8,  # 일대일 관계는 강함
            "1:n": 1.0,  # 기본
            "n:1": 1.0,  # 기본
            "n:m": 1.5,  # 다대다는 약함
            "one": 0.9,
            "many": 1.1,
        }

    def build_graph(self, ontologies: List[OntologyResponse]) -> None:
        """온톨로지들로부터 관계 그래프 구축"""

        logger.info(f"Building path tracking graph from {len(ontologies)} ontologies")

        self.relationship_graph.clear()
        self.reverse_graph.clear()
        self.entities.clear()

        for ontology in ontologies:
            source_entity = ontology.id
            self.entities.add(source_entity)

            for rel in ontology.relationships:
                target_entity = rel.target
                self.entities.add(target_entity)

                # 가중치 계산
                predicate_weight = self.default_weights.get(rel.predicate, 1.0)
                cardinality = str(
                    rel.cardinality.value if hasattr(rel.cardinality, "value") else rel.cardinality
                )
                cardinality_modifier = self.cardinality_weights.get(cardinality, 1.0)
                total_weight = predicate_weight * cardinality_modifier

                # 정방향 홉
                forward_hop = RelationshipHop(
                    source=source_entity,
                    target=target_entity,
                    predicate=rel.predicate,
                    cardinality=cardinality,
                    is_inverse=False,
                    weight=total_weight,
                    metadata={
                        "description": str(rel.description) if rel.description else "",
                        "label": str(rel.label) if rel.label else "",
                    },
                )

                self.relationship_graph[source_entity].append(forward_hop)
                self.reverse_graph[target_entity].append(forward_hop)

                # 역방향 홉 (inverse_predicate가 있는 경우)
                if rel.inverse_predicate:
                    inverse_card = inverse_cardinality(cardinality)
                    inverse_weight = self.default_weights.get(rel.inverse_predicate, 1.0)
                    inverse_cardinality_modifier = self.cardinality_weights.get(
                        inverse_card, 1.0
                    )
                    inverse_total_weight = inverse_weight * inverse_cardinality_modifier

                    backward_hop = RelationshipHop(
                        source=target_entity,
                        target=source_entity,
                        predicate=rel.inverse_predicate,
                        cardinality=inverse_card,
                        is_inverse=True,
                        weight=inverse_total_weight,
                        metadata={
                            "description": str(rel.inverse_label) if rel.inverse_label else "",
                            "original_predicate": rel.predicate,
                        },
                    )

                    self.relationship_graph[target_entity].append(backward_hop)
                    self.reverse_graph[source_entity].append(backward_hop)

        logger.info(
            f"Graph built: {len(self.entities)} entities, "
            f"{sum(len(hops) for hops in self.relationship_graph.values())} relationship hops"
        )

    def find_paths(self, query: PathQuery) -> List[RelationshipPath]:
        """경로 탐색 (쿼리 기반)"""

        path_type_str = (
            query.path_type.value if hasattr(query.path_type, "value") else query.path_type
        )
        logger.info(
            f"Finding paths: {query.start_entity} -> {query.end_entity} "
            f"(type: {path_type_str}, max_depth: {query.max_depth})"
        )

        if query.start_entity not in self.entities:
            logger.warning(f"Start entity '{query.start_entity}' not found in graph")
            return []

        if query.end_entity and query.end_entity not in self.entities:
            logger.warning(f"End entity '{query.end_entity}' not found in graph")
            return []

        # 경로 유형별 탐색
        if query.path_type == PathType.SHORTEST:
            return self._find_shortest_paths(query)
        elif query.path_type == PathType.ALL_PATHS:
            return self._find_all_paths(query)
        elif query.path_type == PathType.WEIGHTED:
            return self._find_weighted_paths(query)
        elif query.path_type == PathType.SEMANTIC:
            return self._find_semantic_paths(query)
        else:
            return []

    def find_shortest_path(
        self, start: str, end: str, max_depth: int = 5
    ) -> Optional[RelationshipPath]:
        """최단 경로 탐색 (단순 버전)"""

        query = PathQuery(
            start_entity=start, end_entity=end, max_depth=max_depth, path_type=PathType.SHORTEST
        )

        paths = self.find_paths(query)
        return paths[0] if paths else None

    def find_all_reachable_entities(
        self, start: str, max_depth: int = 3
    ) -> Dict[str, RelationshipPath]:
        """시작 엔티티에서 도달 가능한 모든 엔티티와 경로"""

        reachable = {}
        visited = set()
        queue = deque([(start, [], 0.0, 0)])  # (entity, hops, weight, depth)

        while queue:
            current_entity, current_hops, current_weight, depth = queue.popleft()

            if current_entity in visited or depth > max_depth:
                continue

            visited.add(current_entity)

            if current_entity != start:
                path = RelationshipPath(
                    start_entity=start,
                    end_entity=current_entity,
                    hops=current_hops,
                    total_weight=current_weight,
                    path_type=PathType.SHORTEST,
                    length=len(current_hops),
                )
                reachable[current_entity] = path

            # 인접 엔티티 탐색
            for hop in self.relationship_graph.get(current_entity, []):
                if hop.target not in visited:
                    new_hops = current_hops + [hop]
                    new_weight = current_weight + hop.weight
                    queue.append((hop.target, new_hops, new_weight, depth + 1))

        return reachable

    def find_connecting_entities(self, entity1: str, entity2: str, max_depth: int = 4) -> List[str]:
        """두 엔티티를 연결하는 중간 엔티티들 탐색"""

        # entity1에서 도달 가능한 엔티티들
        reachable_from_1 = set(self.find_all_reachable_entities(entity1, max_depth // 2).keys())

        # entity2에서 도달 가능한 엔티티들
        reachable_from_2 = set(self.find_all_reachable_entities(entity2, max_depth // 2).keys())

        # 교집합이 중간 연결점
        connecting = list(reachable_from_1.intersection(reachable_from_2))

        return connecting

    def _find_shortest_paths(self, query: PathQuery) -> List[RelationshipPath]:
        """Dijkstra 알고리즘으로 최단 경로 탐색"""

        if not query.end_entity:
            # 단일 목적지가 없으면 모든 도달 가능한 엔티티로의 최단 경로
            reachable = self.find_all_reachable_entities(query.start_entity, query.max_depth)
            return list(reachable.values())

        # 우선순위 큐: (가중치, 엔티티, 홉들)
        pq = [(0.0, query.start_entity, [])]
        visited = set()

        while pq:
            current_weight, current_entity, current_hops = heapq.heappop(pq)

            if current_entity in visited:
                continue

            visited.add(current_entity)

            # 목적지 도달
            if current_entity == query.end_entity:
                return [
                    RelationshipPath(
                        start_entity=query.start_entity,
                        end_entity=query.end_entity,
                        hops=current_hops,
                        total_weight=current_weight,
                        path_type=PathType.SHORTEST,
                        length=len(current_hops),
                    )
                ]

            # 깊이 제한 체크
            if len(current_hops) >= query.max_depth:
                continue

            # 인접 노드 탐색
            for hop in self.relationship_graph.get(current_entity, []):
                if hop.target not in visited and self._is_hop_allowed(hop, query):
                    new_weight = current_weight + self._calculate_hop_weight(hop, query)
                    new_hops = current_hops + [hop]
                    heapq.heappush(pq, (new_weight, hop.target, new_hops))

        return []  # 경로 없음

    def _find_all_paths(self, query: PathQuery) -> List[RelationshipPath]:
        """DFS로 모든 경로 탐색"""

        if not query.end_entity:
            return []

        all_paths = []

        def dfs(current_entity, target, current_hops, current_weight, visited):
            if len(current_hops) >= query.max_depth:
                return

            if current_entity == target and current_hops:
                path = RelationshipPath(
                    start_entity=query.start_entity,
                    end_entity=target,
                    hops=current_hops[:],
                    total_weight=current_weight,
                    path_type=PathType.ALL_PATHS,
                    length=len(current_hops),
                )
                all_paths.append(path)
                return

            if current_entity in visited:
                return

            visited.add(current_entity)

            for hop in self.relationship_graph.get(current_entity, []):
                if self._is_hop_allowed(hop, query):
                    current_hops.append(hop)
                    new_weight = current_weight + self._calculate_hop_weight(hop, query)
                    dfs(hop.target, target, current_hops, new_weight, visited.copy())
                    current_hops.pop()

        dfs(query.start_entity, query.end_entity, [], 0.0, set())

        # 가중치로 정렬
        return sorted(all_paths, key=lambda p: p.total_weight)

    def _find_weighted_paths(self, query: PathQuery) -> List[RelationshipPath]:
        """가중치 기반 최적 경로 탐색"""

        # 커스텀 가중치가 있으면 적용
        if query.weight_predicates:
            original_weights = self.default_weights.copy()
            self.default_weights.update(query.weight_predicates)

            try:
                return self._find_shortest_paths(query)
            finally:
                self.default_weights = original_weights

        return self._find_shortest_paths(query)

    def _find_semantic_paths(self, query: PathQuery) -> List[RelationshipPath]:
        """의미적 관련성 기반 경로 탐색"""

        paths = self._find_all_paths(query)

        # 의미적 점수 계산
        for path in paths:
            path.semantic_score = self._calculate_semantic_score(path, query)

        # 의미적 점수로 정렬
        return sorted(paths, key=lambda p: (-p.semantic_score, p.total_weight))

    def _is_hop_allowed(self, hop: RelationshipHop, query: PathQuery) -> bool:
        """홉이 쿼리 조건에 맞는지 확인"""

        # 방향 체크
        if query.direction == TraversalDirection.FORWARD and hop.is_inverse:
            return False
        if query.direction == TraversalDirection.BACKWARD and not hop.is_inverse:
            return False

        # predicate 포함/제외 체크
        if query.include_predicates and hop.predicate not in query.include_predicates:
            return False
        if query.exclude_predicates and hop.predicate in query.exclude_predicates:
            return False

        # 카디널리티 체크
        if query.include_cardinalities and hop.cardinality not in query.include_cardinalities:
            return False

        return True

    def _calculate_hop_weight(self, hop: RelationshipHop, query: PathQuery) -> float:
        """홉의 가중치 계산"""

        base_weight = hop.weight

        # 커스텀 가중치 적용
        if query.weight_predicates and hop.predicate in query.weight_predicates:
            base_weight = query.weight_predicates[hop.predicate]

        # 의미적 부스트 적용
        if query.semantic_boost and hop.predicate in query.semantic_boost:
            base_weight *= query.semantic_boost[hop.predicate]

        return base_weight

    def _calculate_semantic_score(self, path: RelationshipPath, query: PathQuery) -> float:
        """경로의 의미적 점수 계산"""

        score = 0.0

        # predicate 의미적 강도
        semantic_strengths = {
            "instanceOf": 0.9,
            "hasInstance": 0.9,
            "partOf": 0.8,
            "contains": 0.8,
            "parentOf": 0.7,
            "childOf": 0.7,
            "worksFor": 0.6,
            "hasEmployee": 0.6,
            "related": 0.3,
            "associated": 0.3,
        }

        for hop in path.hops:
            strength = semantic_strengths.get(hop.predicate, 0.5)
            score += strength

        # 경로 길이 페널티 (짧을수록 의미적으로 강함)
        length_penalty = 1.0 / (1.0 + path.length * 0.2)
        score *= length_penalty

        return score

    def get_path_statistics(self, paths: List[RelationshipPath]) -> Dict[str, Any]:
        """경로 통계 정보"""

        if not paths:
            return {"total_paths": 0}

        return {
            "total_paths": len(paths),
            "average_length": sum(p.length for p in paths) / len(paths),
            "min_length": min(p.length for p in paths),
            "max_length": max(p.length for p in paths),
            "average_weight": sum(p.total_weight for p in paths) / len(paths),
            "min_weight": min(p.total_weight for p in paths),
            "max_weight": max(p.total_weight for p in paths),
            "common_predicates": self._find_common_predicates(paths),
            "path_types": list(set(p.path_type.value for p in paths)),
        }

    def _find_common_predicates(self, paths: List[RelationshipPath]) -> List[str]:
        """경로들에서 공통으로 사용되는 predicate 찾기"""

        predicate_counts = defaultdict(int)

        for path in paths:
            unique_predicates = set(path.predicates)
            for predicate in unique_predicates:
                predicate_counts[predicate] += 1

        # 50% 이상의 경로에서 사용되는 predicate
        threshold = len(paths) * 0.5
        common = [pred for pred, count in predicate_counts.items() if count >= threshold]

        return sorted(common, key=lambda p: predicate_counts[p], reverse=True)

    def visualize_path(self, path: RelationshipPath, format: str = "text") -> str:
        """경로 시각화"""

        if format == "text":
            return path.to_readable_string()

        elif format == "json":
            import json

            return json.dumps(
                {
                    "start": path.start_entity,
                    "end": path.end_entity,
                    "path": path.entities,
                    "predicates": path.predicates,
                    "length": path.length,
                    "weight": path.total_weight,
                },
                indent=2,
            )

        elif format == "mermaid":
            # Mermaid 다이어그램 형식
            lines = ["graph LR"]
            for i, hop in enumerate(path.hops):
                lines.append(f"    {hop.source} -->|{hop.predicate}| {hop.target}")
            return "\n".join(lines)

        else:
            return path.to_readable_string()

    def export_graph_summary(self) -> Dict[str, Any]:
        """그래프 요약 정보 내보내기"""

        return {
            "total_entities": len(self.entities),
            "total_relationships": sum(len(hops) for hops in self.relationship_graph.values()),
            "entities": sorted(list(self.entities)),
            "relationship_types": list(
                set(hop.predicate for hops in self.relationship_graph.values() for hop in hops)
            ),
            "cardinality_distribution": self._get_cardinality_distribution(),
            "average_connections_per_entity": (
                sum(len(hops) for hops in self.relationship_graph.values()) / len(self.entities)
                if self.entities
                else 0
            ),
        }

    def _get_cardinality_distribution(self) -> Dict[str, int]:
        """카디널리티 분포 통계"""

        distribution = defaultdict(int)

        for hops in self.relationship_graph.values():
            for hop in hops:
                distribution[hop.cardinality] += 1

        return dict(distribution)
