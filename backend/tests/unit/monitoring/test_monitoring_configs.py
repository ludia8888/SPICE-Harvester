from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml


def _backend_dir() -> Path:
    """
    Resolve the `backend/` directory regardless of where the repo is checked out.

    This avoids brittle `parents[n]` indexing.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        if parent.name != "backend":
            continue
        if (parent / "monitoring").exists():
            return parent
    raise RuntimeError("Unable to locate backend directory from test path")


def _repo_root() -> Path:
    return _backend_dir().parent


@pytest.mark.unit
def test_prometheus_config_yaml_is_valid_and_wired() -> None:
    config_path = _backend_dir() / "monitoring" / "prometheus.yml"
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert isinstance(data, dict)
    assert data.get("global", {}).get("scrape_interval")
    assert "/etc/prometheus/alert_rules.yml" in (data.get("rule_files") or [])

    scrape_configs = data.get("scrape_configs") or []
    assert isinstance(scrape_configs, list)

    job_names = {item.get("job_name") for item in scrape_configs if isinstance(item, dict)}
    assert "spice-services" in job_names
    assert "otel-collector" in job_names
    assert "jaeger" in job_names


@pytest.mark.unit
def test_alert_rules_yaml_is_valid_and_has_minimum_alerts() -> None:
    rules_path = _backend_dir() / "monitoring" / "alert_rules.yml"
    rules = yaml.safe_load(rules_path.read_text(encoding="utf-8"))

    assert isinstance(rules, dict)
    groups = rules.get("groups")
    assert isinstance(groups, list) and groups

    all_rules = []
    for group in groups:
        assert isinstance(group, dict)
        assert group.get("name")
        group_rules = group.get("rules")
        assert isinstance(group_rules, list) and group_rules
        all_rules.extend(group_rules)

    alerts = [rule for rule in all_rules if isinstance(rule, dict) and rule.get("alert")]
    alert_names = {rule["alert"] for rule in alerts}
    assert "SpiceServiceDown" in alert_names
    assert "OtelCollectorDown" in alert_names
    assert "JaegerDown" in alert_names
    assert "SpiceEnterpriseErrorBurst" in alert_names
    assert "SpiceRuntimeFallbackBurst" in alert_names
    assert "SpiceErrorTaxonomyMetricsMissing" in alert_names
    assert "SpiceRuntimeFallbackMetricsMissing" in alert_names
    assert "SpiceObservabilitySignalInactive" in alert_names

    for rule in alerts:
        assert rule.get("expr")
        labels = rule.get("labels") or {}
        assert isinstance(labels, dict)
        annotations = rule.get("annotations") or {}
        assert isinstance(annotations, dict)


@pytest.mark.unit
def test_alertmanager_yaml_is_valid_and_references_default_receiver() -> None:
    config_path = _backend_dir() / "monitoring" / "alertmanager.yml"
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert isinstance(data, dict)
    route = data.get("route") or {}
    assert route.get("receiver") == "default"

    receivers = data.get("receivers") or []
    assert isinstance(receivers, list)
    names = {receiver.get("name") for receiver in receivers if isinstance(receiver, dict)}
    assert "default" in names


@pytest.mark.unit
def test_grafana_dashboard_json_is_valid() -> None:
    path = _backend_dir() / "monitoring" / "grafana_dashboard.json"
    data = json.loads(path.read_text(encoding="utf-8"))

    assert isinstance(data, dict)
    assert data.get("title")
    assert "panels" in data
    assert isinstance(data.get("panels"), list)

    expressions = []
    for panel in data.get("panels") or []:
        targets = panel.get("targets") if isinstance(panel, dict) else []
        if not isinstance(targets, list):
            continue
        for target in targets:
            if isinstance(target, dict) and isinstance(target.get("expr"), str):
                expressions.append(target["expr"])

    joined = "\n".join(expressions)
    assert "spice_errors_total" in joined
    assert "spice_runtime_fallback_total" in joined
    assert "spice_observability_signal_active" in joined
    assert "up{job=\"otel-collector\"}" in joined
    assert "up{job=\"jaeger\"}" in joined


@pytest.mark.unit
def test_operations_doc_mentions_backup_scripts() -> None:
    """
    Guard against ops runbook drift: docs should reference code-backed scripts.
    """
    ops_doc = _repo_root() / "docs" / "OPERATIONS.md"
    if not ops_doc.exists():
        pytest.skip("OPERATIONS.md not present in this checkout")
    text = ops_doc.read_text(encoding="utf-8")
    assert "scripts/ops/backup_postgres.sh" in text
    assert "scripts/ops/backup_minio.sh" in text
    assert "scripts/ops/backup_terminusdb_volume.sh" not in text
