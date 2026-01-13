"""
Operational Memory (P0): Context Pack Builder.

Builds a minimal, safe context payload for LLM planning:
- recent simulations (ActionSimulation)
- recent decisions (ActionLog)
- policy snapshot fingerprints (enterprise catalog + tool allowlist hash)

Security posture:
- Never include raw target docs / patchsets / full inputs.
- Prefer digests and high-level summaries only.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from shared.errors.enterprise_catalog import enterprise_catalog_fingerprint
from shared.services.action_log_registry import ActionLogRegistry
from shared.services.action_simulation_registry import ActionSimulationRegistry
from shared.services.agent_tool_registry import AgentToolRegistry
from shared.utils.canonical_json import sha256_canonical_json_prefixed


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_enterprise_min(result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(result, dict):
        return None
    enterprise = result.get("enterprise")
    if not isinstance(enterprise, dict):
        return None
    out = {
        "code": enterprise.get("code"),
        "class": enterprise.get("class"),
        "severity": enterprise.get("severity"),
        "legacy_code": enterprise.get("legacy_code"),
        "human_required": enterprise.get("human_required"),
        "retryable": enterprise.get("retryable"),
        "default_retry_policy": enterprise.get("default_retry_policy"),
        "safe_next_actions": enterprise.get("safe_next_actions"),
        "catalog_fingerprint": enterprise.get("catalog_fingerprint"),
    }
    return {k: v for k, v in out.items() if v not in (None, "", [], {})} or None


def _summarize_simulation_result(result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {"effective_status": None}
    results = result.get("results")
    if not isinstance(results, list) or not results:
        return {"effective_status": None}
    effective = None
    for item in results:
        if not isinstance(item, dict):
            continue
        if item.get("conflict_policy_override") is None and str(item.get("scenario_id") or "").strip() in {
            "",
            "default",
            "effective",
        }:
            effective = item
            break
    if effective is None:
        effective = results[0] if isinstance(results[0], dict) else None
    status = str((effective or {}).get("status") or "").strip().upper() or None
    error = (effective or {}).get("error")
    error_key = str(error.get("error") or "").strip() if isinstance(error, dict) else None
    enterprise = _extract_enterprise_min(error if isinstance(error, dict) else None)
    return {
        "effective_status": status,
        "error_key": error_key or None,
        "enterprise": enterprise,
    }


async def build_operational_context_pack(
    *,
    db_name: Optional[str],
    actor: str,
    action_type_id: Optional[str],
    action_logs: ActionLogRegistry,
    tool_registry: AgentToolRegistry,
    max_decisions: int = 5,
    max_simulations: int = 5,
) -> Dict[str, Any]:
    db_name = str(db_name or "").strip() or None
    actor = str(actor or "").strip() or "system"
    action_type_id = str(action_type_id or "").strip() or None

    tool_policies = await tool_registry.list_tool_policies(status="ACTIVE", limit=500)
    tool_policy_hash = sha256_canonical_json_prefixed(
        [
            {
                "tool_id": p.tool_id,
                "method": p.method,
                "path": p.path,
                "risk_level": p.risk_level,
                "requires_approval": p.requires_approval,
                "requires_idempotency_key": p.requires_idempotency_key,
                "status": p.status,
                "roles": p.roles,
                "max_payload_bytes": p.max_payload_bytes,
            }
            for p in tool_policies
        ]
    )

    pack: Dict[str, Any] = {
        "generated_at": _now_iso(),
        "db_name": db_name,
        "actor": actor,
        "action_type_id": action_type_id,
        "policy_snapshot": {
            "catalog_fingerprint": enterprise_catalog_fingerprint(),
            "tool_allowlist_hash": tool_policy_hash,
        },
    }

    if db_name:
        decisions = await action_logs.list_logs(
            db_name=db_name,
            action_type_id=action_type_id,
            submitted_by=actor,
            limit=max(1, int(max_decisions)),
            offset=0,
        )
        pack["recent_decisions"] = [
            {
                "action_log_id": rec.action_log_id,
                "action_type_id": rec.action_type_id,
                "status": rec.status,
                "submitted_at": rec.submitted_at.isoformat(),
                "finished_at": rec.finished_at.isoformat() if rec.finished_at else None,
                "error_key": (rec.result or {}).get("error") if isinstance(rec.result, dict) else None,
                "enterprise": _extract_enterprise_min(rec.result if isinstance(rec.result, dict) else None),
            }
            for rec in decisions
        ]

        sims_registry = ActionSimulationRegistry()
        await sims_registry.connect()
        try:
            sims = await sims_registry.list_simulations(
                db_name=db_name, action_type_id=action_type_id, limit=50, offset=0
            )
            sims = [s for s in sims if str(getattr(s, "created_by", "") or "").strip() == actor]
            sims = sims[: max(0, int(max_simulations))]
            recent_sims = []
            for sim in sims:
                versions = await sims_registry.list_versions(simulation_id=sim.simulation_id, limit=1, offset=0)
                latest = versions[0] if versions else None
                sim_summary = {
                    "simulation_id": sim.simulation_id,
                    "action_type_id": sim.action_type_id,
                    "title": sim.title,
                    "created_at": sim.created_at.isoformat(),
                    "updated_at": sim.updated_at.isoformat(),
                }
                if latest:
                    sim_summary.update(
                        {
                            "version": latest.version,
                            "status": latest.status,
                            "base_branch": latest.base_branch,
                            "overlay_branch": latest.overlay_branch,
                            "summary": _summarize_simulation_result(latest.result),
                            "error": _extract_enterprise_min(latest.error if isinstance(latest.error, dict) else None),
                        }
                    )
                recent_sims.append(sim_summary)
            pack["recent_simulations"] = recent_sims
        finally:
            await sims_registry.close()

    return pack

