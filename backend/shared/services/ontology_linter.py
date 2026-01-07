"""
Domain-neutral ontology linter (backend guardrails).

Design goals:
- Deterministic (no LLM required for correctness)
- Domain-neutral defaults (no business-specific dictionaries)
- Explainable output (human-friendly messages + rationale)
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from shared.models.ontology import Property, Relationship
from shared.models.ontology_lint import LintIssue, LintReport, LintSeverity
from shared.utils.branch_utils import get_protected_branches
from shared.i18n import m

_SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class OntologyLinterConfig:
    """Controls strictness (domain-neutral)."""

    require_primary_key: bool = True
    allow_implicit_primary_key: bool = True
    require_title_key: bool = True
    allow_implicit_title_key: bool = True
    block_event_like_class_names: bool = False
    enforce_snake_case_fields: bool = False

    @classmethod
    def from_env(cls, *, branch: Optional[str] = None) -> "OntologyLinterConfig":
        def _truthy(name: str, default: str) -> bool:
            return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}

        def _is_production_env() -> bool:
            raw = (os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("APP_ENVIRONMENT") or "")
            return raw.strip().lower() in {"prod", "production"}

        def _requires_proposals() -> bool:
            return _truthy("ONTOLOGY_REQUIRE_PROPOSALS", "true")

        def _allow_implicit_primary_key_default() -> bool:
            if _is_production_env():
                return False
            if _requires_proposals() and branch:
                protected = get_protected_branches()
                if branch in protected:
                    return False
            return True

        def _allow_implicit_title_key_default() -> bool:
            if _is_production_env():
                return False
            if _requires_proposals() and branch:
                protected = get_protected_branches()
                if branch in protected:
                    return False
            return True

        return cls(
            require_primary_key=_truthy("ONTOLOGY_REQUIRE_PRIMARY_KEY", "true"),
            allow_implicit_primary_key=_truthy(
                "ONTOLOGY_ALLOW_IMPLICIT_PRIMARY_KEY",
                "true" if _allow_implicit_primary_key_default() else "false",
            ),
            require_title_key=_truthy("ONTOLOGY_REQUIRE_TITLE_KEY", "true"),
            allow_implicit_title_key=_truthy(
                "ONTOLOGY_ALLOW_IMPLICIT_TITLE_KEY",
                "true" if _allow_implicit_title_key_default() else "false",
            ),
            block_event_like_class_names=_truthy("ONTOLOGY_BLOCK_EVENT_LIKE_CLASS", "false"),
            enforce_snake_case_fields=_truthy("ONTOLOGY_ENFORCE_SNAKE_CASE_FIELDS", "false"),
        )


def _is_snake_case(value: str) -> bool:
    return bool(value and isinstance(value, str) and _SNAKE_CASE_RE.match(value))


def _tokenize(value: str) -> List[str]:
    if not value:
        return []
    lowered = value.lower()
    return [t for t in re.split(r"[\s\-_./]+", lowered) if t]


def _event_like_triggers(value: str) -> List[str]:
    """
    Conservative "event/state/log-like" hint detector.

    This is intentionally conservative to reduce false positives.
    """
    if not value:
        return []

    triggers: List[str] = []

    english_tokens = {
        "status",
        "state",
        "event",
        "log",
        "history",
        "audit",
        "changed",
        "change",
        "updated",
        "created",
        "deleted",
        "processing",
        "completed",
        "cancelled",
        "canceled",
        "delayed",
    }
    for token in _tokenize(value):
        if token in english_tokens:
            triggers.append(token)

    korean_substrings = ["상태", "이력", "로그", "변경", "처리", "완료", "취소", "지연"]
    for s in korean_substrings:
        if s in value:
            triggers.append(s)

    if value.endswith("됨"):
        triggers.append("~됨")
    if "진행중" in value or value.endswith("중"):
        triggers.append("~중")

    return triggers


def _issue(
    severity: LintSeverity,
    rule_id: str,
    message: str,
    *,
    path: Optional[str] = None,
    suggestion: Optional[str] = None,
    rationale: Optional[str] = None,
    metadata: Optional[Dict] = None,
) -> LintIssue:
    return LintIssue(
        severity=severity,
        rule_id=rule_id,
        message=message,
        path=path,
        suggestion=suggestion,
        rationale=rationale,
        metadata=metadata or {},
    )


def compute_risk_score(
    errors: Sequence[LintIssue],
    warnings: Sequence[LintIssue],
    infos: Sequence[LintIssue],
) -> float:
    score = 0.0
    score += 30.0 * len(errors)
    score += 10.0 * len(warnings)
    score += 2.0 * len(infos)
    return max(0.0, min(100.0, score))


def risk_level(score: float) -> str:
    if score >= 75.0:
        return "critical"
    if score >= 50.0:
        return "high"
    if score >= 25.0:
        return "medium"
    return "low"


def lint_ontology_create(
    *,
    class_id: str,
    label: str,
    abstract: bool,
    properties: Sequence[Property],
    relationships: Sequence[Relationship],
    config: Optional[OntologyLinterConfig] = None,
) -> LintReport:
    """Lint a create payload (no IO)."""
    cfg = config or OntologyLinterConfig.from_env()

    errors: List[LintIssue] = []
    warnings: List[LintIssue] = []
    infos: List[LintIssue] = []

    if not _is_snake_case(class_id):
        warnings.append(
            _issue(
                LintSeverity.WARNING,
                "ONT010",
                m(
                    en="Class ID is not snake_case. (recommended: lowercase + digits + _)",
                    ko="클래스 ID가 snake_case가 아닙니다. (권장: 소문자+숫자+_ 조합)",
                ),
                path="id",
                suggestion=m(en="Example: 'customer' or 'purchase_order'", ko="예: 'customer' 또는 'purchase_order'"),
            )
        )

    if cfg.require_primary_key and not abstract:
        expected_pk = f"{class_id.lower()}_id"
        explicit_pk = [p for p in properties if getattr(p, "primary_key", False)]
        id_like = [p for p in properties if p.name == expected_pk or p.name.endswith("_id")]

        if not explicit_pk:
            if not cfg.allow_implicit_primary_key:
                errors.append(
                    _issue(
                        LintSeverity.ERROR,
                        "ONT001",
                        m(
                            en="Explicit primary_key is required on protected/prod branches.",
                            ko="보호/프로덕션 브랜치에서는 primary_key를 명시해야 합니다.",
                        ),
                        path="properties",
                        suggestion=m(
                            en=f"Set primary_key=true on '{expected_pk}' (or define pk_spec in the object_type resource).",
                            ko=f"'{expected_pk}'에 primary_key=true를 지정하세요 (또는 object_type의 pk_spec로 명시).",
                        ),
                        rationale=m(
                            en="Implicit '*_id' heuristics are disabled to prevent collisions.",
                            ko="암묵적 '*_id' 규칙은 충돌 위험 때문에 비활성화됩니다.",
                        ),
                        metadata={"id_like_fields": [p.name for p in id_like]},
                    )
                )
            elif not id_like:
                errors.append(
                    _issue(
                        LintSeverity.ERROR,
                        "ONT001",
                        m(
                            en="Missing primary key field. Instances must be stably identifiable.",
                            ko="기본키(primary key) 속성이 없습니다. 인스턴스를 안정적으로 식별할 수 있어야 합니다.",
                        ),
                        path="properties",
                        suggestion=m(
                            en=f"Example: add '{expected_pk}' (xsd:string) to properties",
                            ko=f"예: properties에 '{expected_pk}'(xsd:string) 추가",
                        ),
                        rationale=m(
                            en="Without a primary key, duplicates/merges/updates become unreliable.",
                            ko="기본키가 없으면 중복/병합/업데이트 시 데이터가 꼬일 가능성이 큽니다.",
                        ),
                    )
                )
            else:
                warnings.append(
                    _issue(
                        LintSeverity.WARNING,
                        "ONT002",
                        m(
                            en="Implicit '*_id' accepted in dev. Declare primary_key explicitly for production.",
                            ko="개발 환경에서는 '*_id'를 허용하지만, 프로덕션에서는 primary_key를 명시하세요.",
                        ),
                        path="properties",
                        suggestion=m(
                            en="Choose one canonical identifier and set primary_key=true.",
                            ko="가장 대표 식별자 1개에 primary_key=true를 지정하세요",
                        ),
                        metadata={"id_like_fields": [p.name for p in id_like]},
                    )
                )
                if len(id_like) > 1:
                    warnings.append(
                        _issue(
                            LintSeverity.WARNING,
                            "ONT003",
                            m(
                                en="Multiple '*_id' candidates found. Setting primary_key explicitly reduces ambiguity.",
                                ko="여러 개의 '*_id' 후보가 있습니다. primary_key를 명시하면 혼동을 줄일 수 있습니다.",
                            ),
                            path="properties",
                            suggestion=m(
                                en="Choose one canonical identifier and set primary_key=true.",
                                ko="가장 대표 식별자 1개에 primary_key=true를 지정하세요",
                            ),
                        metadata={"id_like_fields": [p.name for p in id_like]},
                    )
                )

    if cfg.require_title_key and not abstract:
        expected_title = f"{class_id.lower()}_name"
        explicit_title = [p for p in properties if getattr(p, "title_key", False)]
        title_like = [
            p
            for p in properties
            if p.name in {"name", "title", "label", expected_title} or p.name.endswith("_name")
        ]

        if len(explicit_title) > 1:
            errors.append(
                _issue(
                    LintSeverity.ERROR,
                    "ONT003",
                    m(
                        en="Multiple title_key fields detected. Only one display key is allowed.",
                        ko="여러 개의 title_key가 지정되었습니다. 표시 키는 하나만 허용됩니다.",
                    ),
                    path="properties",
                    suggestion=m(
                        en="Choose a single title key (e.g., 'name') and set title_key=true on it.",
                        ko="대표 표시 키 1개(예: 'name')만 title_key=true로 지정하세요.",
                    ),
                    metadata={"title_key_fields": [p.name for p in explicit_title]},
                )
            )
        elif not explicit_title:
            if not cfg.allow_implicit_title_key:
                errors.append(
                    _issue(
                        LintSeverity.ERROR,
                        "ONT003",
                        m(
                            en="Explicit title_key is required on protected/prod branches.",
                            ko="보호/프로덕션 브랜치에서는 title_key를 명시해야 합니다.",
                        ),
                        path="properties",
                        suggestion=m(
                            en=f"Set title_key=true on '{expected_title}' or a display field.",
                            ko=f"'{expected_title}' 등 표시용 필드에 title_key=true를 지정하세요.",
                        ),
                        rationale=m(
                            en="Without a title key, objects render without a stable display name.",
                            ko="title_key가 없으면 객체 표시 이름이 불안정해집니다.",
                        ),
                        metadata={"title_like_fields": [p.name for p in title_like]},
                    )
                )
            elif not title_like:
                errors.append(
                    _issue(
                        LintSeverity.ERROR,
                        "ONT003",
                        m(
                            en="Missing title key field. Objects need a stable display name.",
                            ko="title key 속성이 없습니다. 객체 표시 이름이 필요합니다.",
                        ),
                        path="properties",
                        suggestion=m(
                            en="Add a 'name' (xsd:string) property or set title_key=true.",
                            ko="'name'(xsd:string) 속성을 추가하거나 title_key=true를 지정하세요.",
                        ),
                        rationale=m(
                            en="UI/object views depend on a stable display name.",
                            ko="UI/객체 뷰는 안정적인 표시 이름에 의존합니다.",
                        ),
                    )
                )
            else:
                warnings.append(
                    _issue(
                        LintSeverity.WARNING,
                        "ONT004",
                        m(
                            en="Implicit title key accepted in dev. Declare title_key explicitly for production.",
                            ko="개발 환경에서는 암묵적 title key를 허용하지만, 프로덕션에서는 title_key를 명시하세요.",
                        ),
                        path="properties",
                        suggestion=m(
                            en="Choose one display field and set title_key=true.",
                            ko="대표 표시 필드 1개에 title_key=true를 지정하세요.",
                        ),
                        metadata={"title_like_fields": [p.name for p in title_like]},
                    )
                )

    triggers = list({*(_event_like_triggers(label)), *(_event_like_triggers(class_id))})
    if triggers:
        sev = LintSeverity.ERROR if cfg.block_event_like_class_names else LintSeverity.WARNING
        (errors if sev == LintSeverity.ERROR else warnings).append(
            _issue(
                sev,
                "ONT020",
                m(
                    en="Class name looks like a 'state/event/log'. Avoid mixing entities with events/states.",
                    ko="클래스명이 '상태/이벤트/로그'처럼 보입니다. 엔티티(Entity)와 이벤트(Event/State)를 섞지 않는 것을 권장합니다.",
                ),
                path="label",
                suggestion=m(
                    en="Use a noun for entities; model events/states separately (action/event/state enum).",
                    ko="엔티티는 '명사형'으로, 이벤트/상태는 별도 모델(액션/이벤트/상태 열거형)로 분리하세요",
                ),
                rationale=m(
                    en="Entity-centric ontologies improve search/relations/authorization/auditability.",
                    ko="온톨로지는 '현실 개체' 중심이 될수록 검색/관계/권한/감사에 유리합니다.",
                ),
                metadata={"triggers": sorted(triggers)},
            )
        )

    if cfg.enforce_snake_case_fields:
        for idx, prop in enumerate(properties):
            if prop.name and not _is_snake_case(prop.name):
                warnings.append(
                    _issue(
                        LintSeverity.WARNING,
                        "ONT030",
                        m(
                            en="Property name is not snake_case. (recommended)",
                            ko="속성명이 snake_case가 아닙니다. (권장)",
                        ),
                        path=f"properties[{idx}].name",
                        suggestion=m(en="Example: 'total_amount', 'created_at'", ko="예: 'total_amount', 'created_at'"),
                    )
                )

        for idx, rel in enumerate(relationships):
            if rel.predicate and not _is_snake_case(rel.predicate):
                warnings.append(
                    _issue(
                        LintSeverity.WARNING,
                        "ONT031",
                        m(
                            en="Relationship predicate is not snake_case. (recommended)",
                            ko="관계(predicate)명이 snake_case가 아닙니다. (권장)",
                        ),
                        path=f"relationships[{idx}].predicate",
                        suggestion=m(en="Example: 'owned_by', 'belongs_to'", ko="예: 'owned_by', 'belongs_to'"),
                    )
                )

    score = compute_risk_score(errors, warnings, infos)
    return LintReport(
        ok=len(errors) == 0,
        risk_score=score,
        risk_level=risk_level(score),
        errors=errors,
        warnings=warnings,
        infos=infos,
    )


def lint_ontology_update(
    *,
    existing_properties: Sequence[Property],
    existing_relationships: Sequence[Relationship],
    updated_properties: Sequence[Property],
    updated_relationships: Sequence[Relationship],
    config: Optional[OntologyLinterConfig] = None,
) -> LintReport:
    """Lint an update as a diff (no IO)."""
    cfg = config or OntologyLinterConfig.from_env()

    errors: List[LintIssue] = []
    warnings: List[LintIssue] = []
    infos: List[LintIssue] = []

    existing_by_name = {p.name: p for p in existing_properties or []}
    updated_by_name = {p.name: p for p in updated_properties or []}

    removed_props = sorted(set(existing_by_name) - set(updated_by_name))
    if removed_props:
        warnings.append(
            _issue(
                LintSeverity.WARNING,
                "ONT901",
                m(
                    en="Removing properties is a high-risk change. (impacts existing data/queries/projections)",
                    ko="속성 삭제는 고위험 변경입니다. (기존 데이터/쿼리/프로젝션에 영향)",
                ),
                path="properties",
                metadata={"removed_properties": removed_props},
            )
        )

    type_changes: List[Tuple[str, str, str]] = []
    required_changes: List[Tuple[str, bool, bool]] = []
    for name in sorted(set(existing_by_name) & set(updated_by_name)):
        before = existing_by_name[name]
        after = updated_by_name[name]
        if before.type != after.type:
            type_changes.append((name, before.type, after.type))
        if bool(before.required) != bool(after.required):
            required_changes.append((name, bool(before.required), bool(after.required)))

    if type_changes:
        warnings.append(
            _issue(
                LintSeverity.WARNING,
                "ONT902",
                m(
                    en="Changing property types is high-risk. (may break parsing/validation/indexing)",
                    ko="속성 타입 변경은 고위험 변경입니다. (파싱/검증/인덱싱 오류 가능)",
                ),
                path="properties",
                metadata={"type_changes": [{"name": n, "from": f, "to": t} for n, f, t in type_changes]},
            )
        )

    if required_changes:
        warnings.append(
            _issue(
                LintSeverity.WARNING,
                "ONT903",
                m(
                    en="Changing `required` is high-risk. (existing data may fail validation)",
                    ko="required 변경은 고위험 변경입니다. (기존 데이터가 validation에 실패할 수 있음)",
                ),
                path="properties",
                metadata={
                    "required_changes": [{"name": n, "from": f, "to": t} for n, f, t in required_changes]
                },
            )
        )

    if cfg.require_primary_key:
        if cfg.allow_implicit_primary_key:
            existing_pk = [p for p in existing_properties if p.primary_key or p.name.endswith("_id")]
            updated_pk = [p for p in updated_properties if p.primary_key or p.name.endswith("_id")]
        else:
            existing_pk = [p for p in existing_properties if p.primary_key]
            updated_pk = [p for p in updated_properties if p.primary_key]
        if existing_pk and not updated_pk:
            errors.append(
                _issue(
                    LintSeverity.ERROR,
                    "ONT920",
                    m(
                        en="Update removes all primary key candidates. Instances may become unidentifiable.",
                        ko="업데이트 결과 기본키 후보가 0개가 됩니다. 인스턴스 식별이 불가능해질 수 있습니다.",
                    ),
                    path="properties",
                )
            )

    score = compute_risk_score(errors, warnings, infos)
    return LintReport(
        ok=len(errors) == 0,
        risk_score=score,
        risk_level=risk_level(score),
        errors=errors,
        warnings=warnings,
        infos=infos,
    )
