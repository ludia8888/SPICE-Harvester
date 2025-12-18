"""
온톨로지 매핑 제안 서비스
기존 온톨로지와 새로운 데이터 스키마 간의 매핑을 자동으로 제안
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import re
import unicodedata
from difflib import SequenceMatcher
# import jellyfish  # For phonetic matching - removed due to package dependency
from collections import defaultdict, Counter
import statistics
import math

logger = logging.getLogger(__name__)


@dataclass
class MappingCandidate:
    """매핑 후보"""
    source_field: str
    target_field: str
    confidence: float
    match_type: str  # exact, fuzzy, semantic, type_based
    reasons: List[str]


@dataclass
class MappingSuggestion:
    """매핑 제안 결과"""
    mappings: List[MappingCandidate]
    unmapped_source_fields: List[str]
    unmapped_target_fields: List[str]
    overall_confidence: float


class MappingSuggestionService:
    """
    스키마 간 매핑을 자동으로 제안하는 서비스
    
    Features:
    - 이름 기반 매칭 (exact, fuzzy, phonetic)
    - 타입 기반 매칭
    - 의미론적 매칭 (옵션: 일반적인 별칭/동의어)
    - 값 패턴 기반 매칭
    - 값 분포 유사도 매칭
    - 토큰 기반 이름 유사도
    - 신뢰도 점수 계산
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # Configurable thresholds and weights
        default_config = {
            'thresholds': {
                'min_confidence': 0.6,
                'auto_accept': 0.8,
                'fuzzy_match': 0.7,
                'name_similarity': 0.5,
            },
            'weights': {
                'exact_match': 1.0,
                'token_similarity': 0.8,
                'fuzzy_match': 0.7,
                'semantic_match': 0.85,
                'type_match': 0.5,
                'pattern_match': 0.9,
                'distribution_similarity': 0.9,
            },
            'stop_words': {
                'id', 'code', 'name', 'no', 'number', 'key', 'value', 'data', 'info'
            },
            # Domain-neutral defaults: semantic matching is opt-in.
            'features': {
                'semantic_match': False,
            },
            # Optional alias groups for semantic matching (kept intentionally generic).
            'semantic_aliases': {
                'name': ['name', 'full_name', 'fullname', 'display_name', '이름', '성명'],
                'email': ['email', 'email_address', 'e-mail', 'mail', '이메일'],
                'phone': ['phone', 'phone_number', 'tel', 'telephone', 'mobile', '연락처', '전화번호', '휴대폰'],
                'date': ['date', 'datetime', 'timestamp', 'created_at', 'updated_at', '날짜', '일시'],
                'id': ['id', 'identifier', 'code', 'key', '아이디', '식별자'],
            },
        }
        
        self.config = default_config
        if config:
            # Deep merge config
            for key, value in config.items():
                if isinstance(value, dict) and key in self.config:
                    self.config[key].update(value)
                else:
                    self.config[key] = value
        
        self.thresholds = self.config['thresholds']
        self.weights = self.config['weights']

        stop_words = self.config.get('stop_words') or set()
        self.stop_words = stop_words if isinstance(stop_words, set) else set(stop_words)

        self.features = self.config.get('features') or {}
        self.semantic_match_enabled = bool(self.features.get('semantic_match', False))
        self.semantic_aliases = self.config.get('semantic_aliases') or {}
        
        # NOTE: Previously this class shipped with a hard-coded, business-oriented synonym list.
        # For domain-neutral behavior, semantic matching is now opt-in and driven by `semantic_aliases`.
        
        # Type compatibility matrix
        self.type_compatibility = {
            'xsd:string': ['xsd:string', 'text', 'varchar', 'char'],
            'xsd:integer': ['xsd:integer', 'xsd:decimal', 'int', 'number', 'numeric'],
            'xsd:decimal': ['xsd:decimal', 'xsd:integer', 'float', 'double', 'number'],
            'xsd:boolean': ['xsd:boolean', 'bool', 'bit'],
            'xsd:date': ['xsd:date', 'xsd:dateTime', 'date', 'datetime'],
            'xsd:dateTime': ['xsd:dateTime', 'xsd:date', 'datetime', 'timestamp'],
        }
        
    def suggest_mappings(
        self,
        source_schema: List[Dict[str, Any]],
        target_schema: List[Dict[str, Any]],
        sample_data: Optional[List[Dict[str, Any]]] = None,
        target_sample_data: Optional[List[Dict[str, Any]]] = None
    ) -> MappingSuggestion:
        """
        소스 스키마를 타겟 스키마에 매핑하는 제안 생성
        
        Args:
            source_schema: 매핑할 원본 스키마 (새 데이터)
            target_schema: 매핑 대상 스키마 (기존 온톨로지)
            sample_data: 소스 값 패턴 분석을 위한 샘플 데이터
            target_sample_data: 타겟 값 분포 비교를 위한 샘플 데이터
            
        Returns:
            매핑 제안 결과
        """
        all_candidates = []
        
        # 각 소스 필드에 대해 가능한 타겟 매핑 찾기
        for source_field in source_schema:
            field_candidates = []
            
            for target_field in target_schema:
                # 다양한 매칭 방법 시도
                candidates = []
                
                # 1. 정확한 이름 매칭
                exact_match = self._check_exact_match(source_field, target_field)
                if exact_match:
                    candidates.append(exact_match)
                
                # 2. 토큰 기반 이름 매칭
                token_match = self._check_token_match(source_field, target_field)
                if token_match and token_match.confidence > self.thresholds['fuzzy_match']:
                    candidates.append(token_match)
                
                # 3. 퍼지 이름 매칭
                fuzzy_match = self._check_fuzzy_match(source_field, target_field)
                if fuzzy_match and fuzzy_match.confidence > self.thresholds['fuzzy_match']:
                    candidates.append(fuzzy_match)
                
                # 4. 의미론적 매칭 (도메인 지식)
                if self.semantic_match_enabled:
                    semantic_match = self._check_semantic_match(source_field, target_field)
                    if semantic_match:
                        candidates.append(semantic_match)
                
                # 5. 타입 기반 매칭
                type_match = self._check_type_match(source_field, target_field)
                if type_match:
                    candidates.append(type_match)
                
                # 6. 값 패턴 매칭 (샘플 데이터가 있는 경우)
                if sample_data:
                    pattern_match = self._check_pattern_match(
                        source_field, target_field, sample_data
                    )
                    if pattern_match:
                        candidates.append(pattern_match)
                
                # 7. 값 분포 유사도 매칭 (양쪽 샘플 데이터가 있는 경우)
                if sample_data and target_sample_data:
                    dist_match = self._check_distribution_match(
                        source_field, target_field, sample_data, target_sample_data
                    )
                    if dist_match:
                        candidates.append(dist_match)
                
                # 최고 신뢰도 후보 선택
                if candidates:
                    best_candidate = max(candidates, key=lambda x: x.confidence)
                    field_candidates.append(best_candidate)
            
            # 각 소스 필드에 대한 최적 매핑 선택
            if field_candidates:
                best_mapping = max(field_candidates, key=lambda x: x.confidence)
                # 신뢰도 임계값 이상인 경우만 포함
                if best_mapping.confidence >= self.thresholds['min_confidence']:
                    all_candidates.append(best_mapping)
        
        # 중복 제거 및 충돌 해결
        final_mappings = self._resolve_conflicts(all_candidates)
        
        # 매핑되지 않은 필드 찾기
        mapped_sources = {m.source_field for m in final_mappings}
        mapped_targets = {m.target_field for m in final_mappings}
        
        unmapped_sources = [
            f['name'] for f in source_schema 
            if f['name'] not in mapped_sources
        ]
        unmapped_targets = [
            f['name'] for f in target_schema 
            if f['name'] not in mapped_targets
        ]
        
        # 전체 신뢰도 계산
        overall_confidence = (
            sum(m.confidence for m in final_mappings) / len(final_mappings)
            if final_mappings else 0.0
        )
        
        return MappingSuggestion(
            mappings=final_mappings,
            unmapped_source_fields=unmapped_sources,
            unmapped_target_fields=unmapped_targets,
            overall_confidence=overall_confidence
        )

    @staticmethod
    def _field_name_candidates(field: Dict[str, Any]) -> List[str]:
        """
        Return candidate strings for name matching.

        Supports optional metadata keys:
        - label: human-readable label
        - aliases: list of alternative names
        """
        candidates: List[str] = []

        # Primary name is required by the service contract.
        name = field.get("name")
        if name is not None:
            name_str = str(name).strip()
            if name_str:
                candidates.append(name_str)

        label = field.get("label")
        if label is not None:
            label_str = str(label).strip()
            if label_str:
                candidates.append(label_str)

        aliases = field.get("aliases")
        if isinstance(aliases, list):
            for a in aliases:
                if a is None:
                    continue
                a_str = str(a).strip()
                if a_str:
                    candidates.append(a_str)

        seen = set()
        out: List[str] = []
        for c in candidates:
            if c in seen:
                continue
            seen.add(c)
            out.append(c)

        return out

    def _check_exact_match(
        self, source_field: Dict[str, Any], target_field: Dict[str, Any]
    ) -> Optional[MappingCandidate]:
        """정확한 이름 매칭 검사"""
        source_variants = self._field_name_candidates(source_field)
        target_variants = self._field_name_candidates(target_field)

        for s in source_variants:
            s_norm = self._normalize_field_name(s)
            for t in target_variants:
                t_norm = self._normalize_field_name(t)
                if s_norm != t_norm:
                    continue

                reasons = ['Exact name match']
                if s != source_field.get("name") or t != target_field.get("name"):
                    reasons.append(f'Matched "{s}" ↔ "{t}"')

                return MappingCandidate(
                    source_field=source_field['name'],
                    target_field=target_field['name'],
                    confidence=self.weights['exact_match'],
                    match_type='exact',
                    reasons=reasons,
                )
        return None
    
    def _check_token_match(
        self, source_field: Dict[str, Any], target_field: Dict[str, Any]
    ) -> Optional[MappingCandidate]:
        """토큰 기반 이름 매칭 검사"""
        source_variants = self._field_name_candidates(source_field)
        target_variants = self._field_name_candidates(target_field)

        best_score = 0.0
        best_pair = (source_field.get("name", ""), target_field.get("name", ""))
        for s in source_variants:
            for t in target_variants:
                score = self._token_similarity(s, t)
                if score > best_score:
                    best_score = score
                    best_pair = (s, t)

        source_name, target_name = best_pair
        token_sim = best_score
        
        if token_sim > 0.5:  # Minimum threshold for token match
            confidence = token_sim * self.weights['token_similarity']
            reasons = []
            
            source_tokens = self._tokenize_field_name(source_name)
            target_tokens = self._tokenize_field_name(target_name)
            common_tokens = set(source_tokens) & set(target_tokens)
            
            if common_tokens:
                reasons.append(f'Common tokens: {", ".join(sorted(common_tokens))}')
            
            if token_sim > 0.8:
                reasons.append(f'High token similarity ({token_sim:.2f})')
            else:
                reasons.append(f'Moderate token similarity ({token_sim:.2f})')

            if source_name != source_field.get("name") or target_name != target_field.get("name"):
                reasons.append(f'Matched "{source_name}" ↔ "{target_name}"')
            
            return MappingCandidate(
                source_field=source_field['name'],
                target_field=target_field['name'],
                confidence=confidence,
                match_type='token',
                reasons=reasons
            )
        return None
    
    def _check_fuzzy_match(
        self, source_field: Dict[str, Any], target_field: Dict[str, Any]
    ) -> Optional[MappingCandidate]:
        """퍼지 이름 매칭 검사"""
        source_variants = self._field_name_candidates(source_field)
        target_variants = self._field_name_candidates(target_field)

        best_similarity = 0.0
        best_levenshtein = 0.0
        best_score = 0.0
        best_pair = (source_field.get("name", ""), target_field.get("name", ""))

        for s in source_variants:
            s_norm = self._normalize_field_name(s)
            for t in target_variants:
                t_norm = self._normalize_field_name(t)

                similarity = SequenceMatcher(None, s_norm, t_norm).ratio()
                levenshtein_sim = self._levenshtein_similarity(s_norm, t_norm)
                combined_score = (similarity * 0.7 + levenshtein_sim * 0.3)

                if combined_score > best_score:
                    best_score = combined_score
                    best_similarity = similarity
                    best_levenshtein = levenshtein_sim
                    best_pair = (s, t)
        
        if best_score > self.thresholds['fuzzy_match']:
            reasons = []
            if best_similarity > 0.8:
                reasons.append(f'High string similarity ({best_similarity:.2f})')
            if best_levenshtein > 0.8:
                reasons.append(f'Similar spelling (edit distance: {best_levenshtein:.2f})')

            if best_pair[0] != source_field.get("name") or best_pair[1] != target_field.get("name"):
                reasons.append(f'Matched "{best_pair[0]}" ↔ "{best_pair[1]}"')
            
            return MappingCandidate(
                source_field=source_field['name'],
                target_field=target_field['name'],
                confidence=best_score * self.weights['fuzzy_match'],
                match_type='fuzzy',
                reasons=reasons
            )
        return None
    
    def _check_semantic_match(
        self, source_field: Dict[str, Any], target_field: Dict[str, Any]
    ) -> Optional[MappingCandidate]:
        """의미론적 매칭 검사 (옵션: 별칭/동의어 그룹 기반)"""
        source_norms = {
            self._normalize_field_name(v) for v in self._field_name_candidates(source_field)
        }
        target_norms = {
            self._normalize_field_name(v) for v in self._field_name_candidates(target_field)
        }

        # Check alias groups
        for key, variations in (self.semantic_aliases or {}).items():
            normalized_variations = {self._normalize_field_name(key)}
            for v in variations:
                normalized_variations.add(self._normalize_field_name(v))

            if source_norms & normalized_variations and target_norms & normalized_variations:
                return MappingCandidate(
                    source_field=source_field['name'],
                    target_field=target_field['name'],
                    confidence=self.weights['semantic_match'],
                    match_type='semantic',
                    reasons=[f'Both fields match alias group "{key}"'],
                )
        
        # Check if one is abbreviation of the other
        for s in source_norms:
            for t in target_norms:
                if self._is_abbreviation(s, t):
                    return MappingCandidate(
                        source_field=source_field['name'],
                        target_field=target_field['name'],
                        confidence=0.75 * self.weights['semantic_match'],
                        match_type='semantic',
                        reasons=['Abbreviation match'],
                    )
        
        return None
    
    def _check_type_match(
        self, source_field: Dict[str, Any], target_field: Dict[str, Any]
    ) -> Optional[MappingCandidate]:
        """타입 기반 매칭 검사"""
        source_type = source_field.get('type', 'xsd:string')
        target_type = target_field.get('type', 'xsd:string')
        
        # Check if types are compatible
        compatible = False
        for base_type, compatible_types in self.type_compatibility.items():
            if source_type in compatible_types and target_type in compatible_types:
                compatible = True
                break
        
        if not compatible:
            return None
        
        # Type-based matching is weak, so only suggest if names are somewhat similar
        source_variants = self._field_name_candidates(source_field)
        target_variants = self._field_name_candidates(target_field)

        best_similarity = 0.0
        best_pair = (source_field.get("name", ""), target_field.get("name", ""))
        for s in source_variants:
            s_norm = self._normalize_field_name(s)
            for t in target_variants:
                t_norm = self._normalize_field_name(t)
                sim = SequenceMatcher(None, s_norm, t_norm).ratio()
                if sim > best_similarity:
                    best_similarity = sim
                    best_pair = (s, t)
        name_similarity = best_similarity
        
        if name_similarity > self.thresholds['name_similarity']:
            confidence = (0.5 + (name_similarity * 0.3)) * self.weights['type_match']
            reasons = [
                f'Compatible types ({source_type} → {target_type})',
                f'Partial name similarity ({name_similarity:.2f})',
            ]
            if best_pair[0] != source_field.get("name") or best_pair[1] != target_field.get("name"):
                reasons.append(f'Matched "{best_pair[0]}" ↔ "{best_pair[1]}"')
            return MappingCandidate(
                source_field=source_field['name'],
                target_field=target_field['name'],
                confidence=confidence,
                match_type='type_based',
                reasons=reasons,
            )
        
        return None
    
    def _check_pattern_match(
        self, source_field: Dict[str, Any], 
        target_field: Dict[str, Any],
        sample_data: List[Dict[str, Any]]
    ) -> Optional[MappingCandidate]:
        """값 패턴 기반 매칭 검사"""
        source_name = source_field['name']
        target_text = " ".join(self._field_name_candidates(target_field)).lower()
        
        # Get sample values
        source_values = [
            row.get(source_name) for row in sample_data 
            if row.get(source_name) is not None
        ][:100]  # Limit to 100 samples
        
        if not source_values:
            return None
        
        # Analyze value patterns
        source_patterns = self._analyze_value_patterns(source_values)
        
        # Compare with target field expected patterns
        confidence = 0.0
        reasons = []
        
        # Email pattern
        if source_patterns['is_email'] and any(term in target_text for term in ['email', 'e-mail', 'mail', '이메일']):
            confidence = 0.9 * self.weights['pattern_match']
            reasons.append('Values match email pattern')
        
        # Phone pattern
        elif source_patterns['is_phone'] and any(
            term in target_text for term in ['phone', 'tel', 'mobile', '전화', '연락처', '휴대폰']
        ):
            confidence = 0.9 * self.weights['pattern_match']
            reasons.append('Values match phone pattern')
        
        # Date pattern
        elif source_patterns['is_date'] and any(
            term in target_text for term in ['date', 'time', 'created', 'updated', '날짜', '일시']
        ):
            confidence = 0.85 * self.weights['pattern_match']
            reasons.append('Values match date pattern')
        
        # ID pattern
        elif source_patterns['is_id'] and any(
            term in target_text for term in ['id', 'key', 'code', '아이디', '식별자']
        ):
            confidence = 0.8 * self.weights['pattern_match']
            reasons.append('Values match ID pattern')
        
        if confidence > 0:
            return MappingCandidate(
                source_field=source_name,
                target_field=target_field['name'],
                confidence=confidence,
                match_type='pattern',
                reasons=reasons
            )
        
        return None
    
    def _normalize_field_name(self, name: str) -> str:
        """필드 이름 정규화"""
        # Convert to lowercase
        name = name.lower()
        # Replace separators with underscore
        name = re.sub(r'[-\s\.]', '_', name)
        # Remove extra underscores
        name = re.sub(r'_+', '_', name)
        # Remove leading/trailing underscores
        name = name.strip('_')
        return name
    
    def _is_abbreviation(self, short: str, long: str) -> bool:
        """약어 관계 검사"""
        if len(short) >= len(long):
            return False
        
        # Check if short is an abbreviation of long
        # e.g., "dept" is abbreviation of "department"
        if long.startswith(short):
            return True
        
        # Check if it's an acronym
        # e.g., "dob" for "date_of_birth"
        long_parts = long.split('_')
        if len(long_parts) > 1:
            acronym = ''.join(part[0] for part in long_parts)
            if short == acronym:
                return True
        
        return False
    
    def _analyze_value_patterns(self, values: List[Any]) -> Dict[str, bool]:
        """값 패턴 분석"""
        patterns = {
            'is_email': False,
            'is_phone': False,
            'is_date': False,
            'is_id': False,
            'is_numeric': False,
        }
        
        if not values:
            return patterns
        
        # Check patterns
        email_pattern = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]+$')
        # Enhanced phone pattern for international formats
        phone_patterns = [
            re.compile(r'^[\d\s\-\+\(\)\.]+$'),  # General pattern
            re.compile(r'^\+?\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{4}$'),  # International
            re.compile(r'^0\d{1,2}[\s\-]?\d{3,4}[\s\-]?\d{4}$'),  # Korean
        ]
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}')
        id_pattern = re.compile(r'^[A-Z0-9\-\_]{6,}$', re.IGNORECASE)
        
        email_count = sum(1 for v in values if isinstance(v, str) and email_pattern.match(v))
        phone_count = sum(1 for v in values 
                         if isinstance(v, str) and 
                         any(pattern.match(v) for pattern in phone_patterns) and 
                         len(re.sub(r'[^\d]', '', v)) >= 7)
        date_count = sum(1 for v in values if isinstance(v, str) and self._is_date_pattern(v))
        id_count = sum(1 for v in values if isinstance(v, str) and id_pattern.match(v))
        numeric_count = sum(1 for v in values if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').replace('-', '').isdigit()))
        
        total = len(values)
        
        patterns['is_email'] = email_count / total > 0.8
        patterns['is_phone'] = phone_count / total > 0.8
        patterns['is_date'] = date_count / total > 0.8
        patterns['is_id'] = id_count / total > 0.8
        patterns['is_numeric'] = numeric_count / total > 0.8
        
        return patterns
    
    def _resolve_conflicts(self, candidates: List[MappingCandidate]) -> List[MappingCandidate]:
        """매핑 충돌 해결 (하나의 타겟에 여러 소스가 매핑되는 경우)"""
        # Group by target field
        target_groups = defaultdict(list)
        for candidate in candidates:
            target_groups[candidate.target_field].append(candidate)
        
        # Resolve conflicts
        final_mappings = []
        for target_field, mappings in target_groups.items():
            if len(mappings) == 1:
                final_mappings.append(mappings[0])
            else:
                # Choose the one with highest confidence
                best_mapping = max(mappings, key=lambda x: x.confidence)
                final_mappings.append(best_mapping)
                
                # Log conflict resolution
                logger.info(
                    f"Resolved conflict for target '{target_field}': "
                    f"chose '{best_mapping.source_field}' "
                    f"(confidence: {best_mapping.confidence:.2f})"
                )
        
        return final_mappings
    
    def _levenshtein_similarity(self, s1: str, s2: str) -> float:
        """Calculate Levenshtein similarity between two strings"""
        if s1 == s2:
            return 1.0
        
        len1 = len(s1)
        len2 = len(s2)
        
        if len1 == 0 or len2 == 0:
            return 0.0
        
        # Create distance matrix
        dist = [[0] * (len2 + 1) for _ in range(len1 + 1)]
        
        # Initialize first column and row
        for i in range(len1 + 1):
            dist[i][0] = i
        for j in range(len2 + 1):
            dist[0][j] = j
        
        # Calculate distances
        for i in range(1, len1 + 1):
            for j in range(1, len2 + 1):
                if s1[i-1] == s2[j-1]:
                    cost = 0
                else:
                    cost = 1
                
                dist[i][j] = min(
                    dist[i-1][j] + 1,      # deletion
                    dist[i][j-1] + 1,      # insertion
                    dist[i-1][j-1] + cost  # substitution
                )
        
        # Convert distance to similarity (0 to 1)
        max_len = max(len1, len2)
        similarity = 1 - (dist[len1][len2] / max_len)
        
        return similarity
    
    def _tokenize_field_name(self, name: str) -> List[str]:
        """필드 이름을 토큰으로 분리 (스톱워드 제거)"""
        # Normalize first
        normalized = self._normalize_field_name(name)
        
        # Split by various separators including camelCase
        # Convert camelCase to snake_case first
        snake_case = re.sub('([A-Z]+)', r'_\1', normalized).lower()
        
        # Split by underscores, spaces, dots, dashes
        tokens = re.split(r'[_\s\.\-]+', snake_case)
        
        # Remove empty tokens and stop words
        tokens = [t for t in tokens if t and t not in self.stop_words]
        
        return tokens
    
    def _token_similarity(self, source_name: str, target_name: str) -> float:
        """토큰 기반 이름 유사도 계산"""
        source_tokens = set(self._tokenize_field_name(source_name))
        target_tokens = set(self._tokenize_field_name(target_name))
        
        if not source_tokens or not target_tokens:
            return 0.0
        
        # Jaccard similarity
        intersection = len(source_tokens & target_tokens)
        union = len(source_tokens | target_tokens)
        
        if union == 0:
            return 0.0
        
        jaccard = intersection / union
        
        # Boost if all tokens from one side are contained in the other
        containment_score = max(
            intersection / len(source_tokens) if source_tokens else 0,
            intersection / len(target_tokens) if target_tokens else 0
        )
        
        # Weighted combination
        return (jaccard * 0.6 + containment_score * 0.4)
    
    def _normalize_text_advanced(self, text: str) -> str:
        """고급 텍스트 정규화 (NFKC, 공백, 특수문자)"""
        if not isinstance(text, str):
            text = str(text)
        
        # NFKC normalization
        text = unicodedata.normalize('NFKC', text)
        
        # Remove thousand separators and normalize decimals
        text = re.sub(r'(\d),(\d)', r'\1\2', text)  # Remove comma thousand separator
        text = re.sub(r'(\d)\.(\d{3})', r'\1\2', text)  # Remove dot thousand separator
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    def _distribution_similarity(
        self, 
        source_values: List[Any], 
        target_values: List[Any],
        source_type: str,
        target_type: str
    ) -> float:
        """값 분포 유사도 계산"""
        if not source_values or not target_values:
            return 0.0
        
        # Normalize values
        source_values = [self._normalize_text_advanced(v) for v in source_values if v is not None]
        target_values = [self._normalize_text_advanced(v) for v in target_values if v is not None]
        
        if not source_values or not target_values:
            return 0.0
        
        # Numeric distribution similarity
        if source_type in ['xsd:integer', 'xsd:decimal'] and target_type in ['xsd:integer', 'xsd:decimal']:
            return self._numeric_distribution_similarity(source_values, target_values)
        
        # Categorical distribution similarity
        elif source_type in ['xsd:string'] and target_type in ['xsd:string']:
            # Check if values are actually categorical (limited unique values)
            source_unique_ratio = len(set(source_values)) / len(source_values)
            target_unique_ratio = len(set(target_values)) / len(target_values)
            
            if source_unique_ratio < 0.1 and target_unique_ratio < 0.1:
                return self._categorical_distribution_similarity(source_values, target_values)
            else:
                return self._string_distribution_similarity(source_values, target_values)
        
        # Date distribution similarity
        elif source_type in ['xsd:date', 'xsd:dateTime'] and target_type in ['xsd:date', 'xsd:dateTime']:
            return self._temporal_distribution_similarity(source_values, target_values)
        
        # Default: string similarity
        return self._string_distribution_similarity(source_values, target_values)
    
    def _numeric_distribution_similarity(self, source_values: List[str], target_values: List[str]) -> float:
        """수치형 분포 유사도 (KS-test 기반)"""
        try:
            source_nums = [float(v) for v in source_values if self._is_numeric(v)]
            target_nums = [float(v) for v in target_values if self._is_numeric(v)]
            
            if len(source_nums) < 5 or len(target_nums) < 5:
                return 0.0
            
            # Normalize to [0, 1] range
            source_min, source_max = min(source_nums), max(source_nums)
            target_min, target_max = min(target_nums), max(target_nums)
            
            if source_max == source_min or target_max == target_min:
                return 0.0
            
            source_norm = [(x - source_min) / (source_max - source_min) for x in source_nums]
            target_norm = [(x - target_min) / (target_max - target_min) for x in target_nums]
            
            # Simple KS statistic approximation
            source_sorted = sorted(source_norm)
            target_sorted = sorted(target_norm)
            
            # Compare CDFs at multiple points
            n_points = 20
            max_diff = 0.0
            
            for i in range(n_points):
                point = i / n_points
                source_cdf = sum(1 for x in source_sorted if x <= point) / len(source_sorted)
                target_cdf = sum(1 for x in target_sorted if x <= point) / len(target_sorted)
                max_diff = max(max_diff, abs(source_cdf - target_cdf))
            
            # Convert KS statistic to similarity
            similarity = 1.0 - max_diff
            
            # Also consider mean and std similarity
            source_mean = statistics.mean(source_norm)
            target_mean = statistics.mean(target_norm)
            mean_sim = 1.0 - abs(source_mean - target_mean)
            
            source_std = statistics.stdev(source_norm) if len(source_norm) > 1 else 0
            target_std = statistics.stdev(target_norm) if len(target_norm) > 1 else 0
            std_sim = 1.0 - abs(source_std - target_std)
            
            # Weighted combination
            return (similarity * 0.5 + mean_sim * 0.3 + std_sim * 0.2)
            
        except (ValueError, statistics.StatisticsError):
            return 0.0
    
    def _categorical_distribution_similarity(self, source_values: List[str], target_values: List[str]) -> float:
        """범주형 분포 유사도 (Jaccard/Overlap)"""
        source_counts = Counter(source_values)
        target_counts = Counter(target_values)
        
        # Get top N categories
        n_top = 20
        source_top = set(cat for cat, _ in source_counts.most_common(n_top))
        target_top = set(cat for cat, _ in target_counts.most_common(n_top))
        
        if not source_top or not target_top:
            return 0.0
        
        # Jaccard similarity of top categories
        intersection = len(source_top & target_top)
        union = len(source_top | target_top)
        jaccard = intersection / union if union > 0 else 0.0
        
        # Overlap coefficient (good for different-sized sets)
        overlap = intersection / min(len(source_top), len(target_top))
        
        return (jaccard * 0.6 + overlap * 0.4)
    
    def _string_distribution_similarity(self, source_values: List[str], target_values: List[str]) -> float:
        """문자열 분포 유사도 (n-gram 기반)"""
        # Sample values for performance
        sample_size = min(100, len(source_values), len(target_values))
        source_sample = source_values[:sample_size]
        target_sample = target_values[:sample_size]
        
        # Character n-grams (trigrams)
        source_ngrams = Counter()
        target_ngrams = Counter()
        
        for value in source_sample:
            if len(value) >= 3:
                for i in range(len(value) - 2):
                    source_ngrams[value[i:i+3]] += 1
        
        for value in target_sample:
            if len(value) >= 3:
                for i in range(len(value) - 2):
                    target_ngrams[value[i:i+3]] += 1
        
        if not source_ngrams or not target_ngrams:
            return 0.0
        
        # Cosine similarity of n-gram vectors
        common_ngrams = set(source_ngrams.keys()) & set(target_ngrams.keys())
        
        if not common_ngrams:
            return 0.0
        
        dot_product = sum(source_ngrams[ng] * target_ngrams[ng] for ng in common_ngrams)
        source_magnitude = math.sqrt(sum(c * c for c in source_ngrams.values()))
        target_magnitude = math.sqrt(sum(c * c for c in target_ngrams.values()))
        
        if source_magnitude == 0 or target_magnitude == 0:
            return 0.0
        
        cosine_sim = dot_product / (source_magnitude * target_magnitude)
        
        # Also consider average string length similarity
        source_avg_len = statistics.mean(len(v) for v in source_sample)
        target_avg_len = statistics.mean(len(v) for v in target_sample)
        len_sim = 1.0 - abs(source_avg_len - target_avg_len) / max(source_avg_len, target_avg_len)
        
        return (cosine_sim * 0.7 + len_sim * 0.3)
    
    def _temporal_distribution_similarity(self, source_values: List[str], target_values: List[str]) -> float:
        """시간형 분포 유사도"""
        # Simple approach: compare date ranges and patterns
        source_dates = [v for v in source_values if self._is_date_pattern(v)]
        target_dates = [v for v in target_values if self._is_date_pattern(v)]
        
        if not source_dates or not target_dates:
            return 0.0
        
        # Check format similarity
        source_formats = Counter(self._detect_date_format(d) for d in source_dates[:10])
        target_formats = Counter(self._detect_date_format(d) for d in target_dates[:10])
        
        format_match = len(set(source_formats.keys()) & set(target_formats.keys())) > 0
        
        return 0.8 if format_match else 0.2
    
    def _is_numeric(self, value: str) -> bool:
        """Check if string represents a number"""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def _is_date_pattern(self, value: str) -> bool:
        """Check if string matches common date patterns"""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
            r'^\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
            r'^\d{4}\.\d{2}\.\d{2}',  # YYYY.MM.DD
            r'^\d{2}\.\d{2}\.\d{4}',  # DD.MM.YYYY
            r'^\d{8}$',  # YYYYMMDD
            r'^\d{4}년\s*\d{1,2}월\s*\d{1,2}일',  # Korean date
            r'^\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}',  # DD Mon YYYY
        ]
        return any(re.match(pattern, value, re.IGNORECASE) for pattern in date_patterns)
    
    def _detect_date_format(self, date_str: str) -> str:
        """Detect date format pattern"""
        if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
            return 'YYYY-MM-DD'
        elif re.match(r'^\d{2}/\d{2}/\d{4}', date_str):
            return 'MM/DD/YYYY'
        elif re.match(r'^\d{2}-\d{2}-\d{4}', date_str):
            return 'DD-MM-YYYY'
        elif re.match(r'^\d{4}/\d{2}/\d{2}', date_str):
            return 'YYYY/MM/DD'
        return 'unknown'
    
    def _check_distribution_match(
        self, 
        source_field: Dict[str, Any], 
        target_field: Dict[str, Any],
        sample_data: List[Dict[str, Any]],
        target_sample_data: List[Dict[str, Any]]
    ) -> Optional[MappingCandidate]:
        """값 분포 기반 매칭 검사"""
        source_name = source_field['name']
        target_name = target_field['name']
        
        # Get sample values
        source_values = [
            row.get(source_name) for row in sample_data 
            if row.get(source_name) is not None
        ][:200]  # Limit samples
        
        target_values = [
            row.get(target_name) for row in target_sample_data 
            if row.get(target_name) is not None
        ][:200]  # Limit samples
        
        if not source_values or not target_values:
            return None
        
        # Calculate distribution similarity
        dist_sim = self._distribution_similarity(
            source_values, 
            target_values,
            source_field.get('type', 'xsd:string'),
            target_field.get('type', 'xsd:string')
        )
        
        if dist_sim > 0.6:  # Minimum threshold
            confidence = dist_sim * self.weights['distribution_similarity']
            reasons = []
            
            # Determine the type of distribution match
            source_type = source_field.get('type', 'xsd:string')
            target_type = target_field.get('type', 'xsd:string')
            
            if source_type in ['xsd:integer', 'xsd:decimal'] and target_type in ['xsd:integer', 'xsd:decimal']:
                reasons.append(f'Numeric distribution match ({dist_sim:.2f})')
            elif dist_sim > 0.9:
                reasons.append(f'Very high value distribution similarity ({dist_sim:.2f})')
            elif dist_sim > 0.8:
                reasons.append(f'High value distribution similarity ({dist_sim:.2f})')
            else:
                reasons.append(f'Moderate value distribution similarity ({dist_sim:.2f})')
            
            # Add some sample value comparison
            source_sample = set(str(v) for v in source_values[:5])
            target_sample = set(str(v) for v in target_values[:5])
            common_values = source_sample & target_sample
            if common_values:
                reasons.append(f'Common values found: {", ".join(list(common_values)[:3])}')
            
            return MappingCandidate(
                source_field=source_field['name'],
                target_field=target_field['name'],
                confidence=confidence,
                match_type='distribution',
                reasons=reasons
            )
        
        return None
