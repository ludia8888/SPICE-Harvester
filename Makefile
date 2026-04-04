SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# Best-effort: load local compose port overrides / tokens (ignored in CI if missing).
-include .env

PYTHON ?= $(shell \
	if command -v python3 >/dev/null 2>&1 && python3 -m pytest --version >/dev/null 2>&1; then \
		echo python3; \
	elif command -v python >/dev/null 2>&1 && python -m pytest --version >/dev/null 2>&1; then \
		echo python; \
	else \
		echo python3; \
	fi \
)
NPM ?= npm
FRONTEND_DIR ?= frontend
COMPOSE ?= docker compose
COMPOSE_FULL ?= docker-compose.full.yml
COVERAGE_MIN ?= 30.0

.PHONY: help
help:
	@echo "Targets:"
	@echo "  backend-unit           Run fast backend unit tests (no docker required)"
	@echo "  backend-coverage       Run unit tests + generate coverage.xml (for Codecov)"
	@echo "  backend-runtime-ddl-audit  Guard runtime DDL callsites (migration-first)"
	@echo "  backend-platform-contract-audit  Guard shared write-path, health vocabulary, and facade ratchets"
	@echo "  backend-release-gate   Run the full backend release regression gate"
	@echo "  backend-boundary-smoke  Run explicit Redis/Postgres/LakeFS/Kafka/S3 boundary smoke suite"
	@echo "  backend-release-smoke  Run release smoke suite against a running local stack"
	@echo "  backend-infra-tier     Run requires_infra regression tier against a running local stack"
	@echo "  backend-error-taxonomy Run enterprise error taxonomy audit gate"
	@echo "  backend-prod-quick     Run correctness layer tests (Postgres-backed)"
	@echo "  backend-prod-full      Run full backend production gate (requires local stack)"
	@echo "  backend-chaos-lite     Run destructive chaos-lite scenario (requires local stack)"
	@echo "  backend-perf-k6-smoke  Run k6 async command smoke load (requires local stack)"
	@echo "  backend-perf-cleanup   Cleanup perf_* DBs (requires Postgres + BFF)"
	@echo "  backend-methods        Regenerate docs/BACKEND_METHODS.md"
	@echo "  backend-methods-check  Verify docs/BACKEND_METHODS.md is up to date"
	@echo "  docs-update            Regenerate auto-managed docs"
	@echo "  docs-check             Verify auto-managed docs + Sphinx build"
	@echo "  frontend-check         Install + lint + build frontend"
	@echo "  frontend-coverage      Run frontend tests + coverage gate"
	@echo "  ci                     Run backend-coverage + frontend-check + frontend-coverage"
	@echo "  stack-up               Start full local docker stack"
	@echo "  stack-up-build         Start stack (with --build)"
	@echo "  stack-down             Stop stack + clean docker caches"
	@echo "  stack-gc               Prune docker caches (safe)"

.PHONY: backend-unit
backend-unit:
	PYTHONPATH=backend $(PYTHON) backend/scripts/runtime_ddl_audit.py
	PYTHONPATH=backend $(PYTHON) backend/shared/tools/error_taxonomy_audit.py --backend-root backend --fail-on-raw-http-without-code --fail-on-raw-code
	PYTHONPATH=backend $(PYTHON) backend/scripts/platform_contract_audit.py --repo-root .
	PYTHONPATH=backend $(PYTHON) -m pytest -q -c backend/pytest.ini -m "not requires_infra" \
		backend/tests/unit backend/bff/tests backend/funnel/tests

.PHONY: backend-coverage
backend-coverage:
	rm -f coverage.xml || true
	PYTHONPATH=backend $(PYTHON) backend/scripts/runtime_ddl_audit.py
	PYTHONPATH=backend $(PYTHON) backend/shared/tools/error_taxonomy_audit.py --backend-root backend --fail-on-raw-http-without-code --fail-on-raw-code
	PYTHONPATH=backend $(PYTHON) backend/scripts/platform_contract_audit.py --repo-root .
	PYTHONPATH=backend $(PYTHON) -m pytest -q -c backend/pytest.ini -m "not requires_infra" \
		backend/tests/unit backend/bff/tests backend/funnel/tests \
		--cov=backend --cov-config=backend/.coveragerc \
		--cov-branch --cov-report=term-missing:skip-covered --cov-report=xml:coverage.xml \
		--cov-fail-under=$(COVERAGE_MIN)

.PHONY: backend-runtime-ddl-audit
backend-runtime-ddl-audit:
	PYTHONPATH=backend $(PYTHON) backend/scripts/runtime_ddl_audit.py

.PHONY: backend-platform-contract-audit
backend-platform-contract-audit:
	PYTHONPATH=backend $(PYTHON) backend/scripts/platform_contract_audit.py --repo-root .

.PHONY: backend-release-gate
backend-release-gate:
	PYTHONPATH=backend $(PYTHON) backend/scripts/release_regression_gate.py --execute

.PHONY: backend-boundary-smoke
backend-boundary-smoke:
	PYTHONPATH=backend ALLOW_RUNTIME_DDL_BOOTSTRAP=false $(PYTHON) -m pytest -q -c backend/pytest.ini \
		backend/tests/test_infra_boundary_smoke.py

.PHONY: backend-error-taxonomy
backend-error-taxonomy:
	PYTHONPATH=backend $(PYTHON) backend/shared/tools/error_taxonomy_audit.py --backend-root backend --fail-on-raw-http-without-code --fail-on-raw-code

.PHONY: backend-prod-quick
backend-prod-quick:
	PYTHON_BIN="$(if $(findstring /,$(PYTHON)),$(abspath $(PYTHON)),$(PYTHON))" ./backend/run_production_tests.sh --quick

.PHONY: backend-release-smoke
backend-release-smoke:
	PYTHONPATH=backend ALLOW_RUNTIME_DDL_BOOTSTRAP=false $(PYTHON) -m pytest -q -c backend/pytest.ini \
		backend/tests/test_infra_boundary_smoke.py \
		backend/tests/test_mcp_isolation_release_smoke.py \
		backend/tests/test_oms_smoke.py \
		backend/tests/test_openapi_contract_smoke.py \
		backend/tests/test_command_status_ttl_e2e.py \
		backend/tests/test_worker_lease_safety_e2e.py \
		backend/tests/integration/test_pipeline_branch_lifecycle.py

.PHONY: backend-infra-tier
backend-infra-tier:
	PYTHONPATH=backend ALLOW_RUNTIME_DDL_BOOTSTRAP=false $(PYTHON) -m pytest -q -c backend/pytest.ini -m "requires_infra and not smoke" backend/tests

.PHONY: backend-prod-full
backend-prod-full:
	PYTHON_BIN="$(if $(findstring /,$(PYTHON)),$(abspath $(PYTHON)),$(PYTHON))" ./backend/run_production_tests.sh --full

.PHONY: backend-chaos-lite
backend-chaos-lite:
	PYTHON_BIN="$(if $(findstring /,$(PYTHON)),$(abspath $(PYTHON)),$(PYTHON))" ./backend/run_production_tests.sh --full --chaos-lite

.PHONY: backend-perf-k6-smoke
backend-perf-k6-smoke:
	K6_VUS="$${K6_VUS:-3}" K6_ITERATIONS="$${K6_ITERATIONS:-20}" K6_MAX_DURATION="$${K6_MAX_DURATION:-10m}" \
		./backend/perf/run_k6_async_instance_flow.sh

.PHONY: backend-perf-cleanup
backend-perf-cleanup:
	PYTHONPATH=backend $(PYTHON) backend/perf/cleanup_perf_databases.py --prefix perf_ --yes

.PHONY: backend-methods
backend-methods:
	$(PYTHON) scripts/generate_backend_methods.py

.PHONY: backend-methods-check
backend-methods-check:
	$(PYTHON) scripts/generate_backend_methods.py --check

.PHONY: docs-update
docs-update:
	$(PYTHON) scripts/update_docs.py

.PHONY: docs-check
docs-check:
	$(PYTHON) scripts/check_docs.py

.PHONY: frontend-check
frontend-check:
	cd $(FRONTEND_DIR) && $(NPM) ci
	cd $(FRONTEND_DIR) && $(NPM) run lint
	cd $(FRONTEND_DIR) && $(NPM) run build

.PHONY: frontend-coverage
frontend-coverage:
	cd $(FRONTEND_DIR) && $(NPM) ci
	cd $(FRONTEND_DIR) && $(NPM) run test

.PHONY: ci
ci: backend-coverage frontend-check frontend-coverage

.PHONY: stack-up
stack-up:
	$(COMPOSE) -f $(COMPOSE_FULL) up -d

.PHONY: stack-up-build
stack-up-build:
	$(COMPOSE) -f $(COMPOSE_FULL) up -d --build

.PHONY: stack-down
stack-down:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/ops/compose_down_clean.sh -f $(COMPOSE_FULL)

.PHONY: stack-gc
stack-gc:
	./scripts/ops/docker_gc.sh
