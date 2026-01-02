SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# Best-effort: load local compose port overrides / tokens (ignored in CI if missing).
-include .env

PYTHON ?= python3
NPM ?= npm
FRONTEND_DIR ?= frontend
COMPOSE ?= docker compose
COMPOSE_FULL ?= docker-compose.full.yml
COVERAGE_MIN ?= 33.9

.PHONY: help
help:
	@echo "Targets:"
	@echo "  backend-unit           Run fast backend unit tests (no docker required)"
	@echo "  backend-coverage       Run unit tests + generate coverage.xml (for Codecov)"
	@echo "  backend-prod-quick     Run correctness layer tests (Postgres-backed)"
	@echo "  backend-prod-full      Run full backend production gate (requires local stack)"
	@echo "  backend-chaos-lite     Run destructive chaos-lite scenario (requires local stack)"
	@echo "  backend-perf-k6-smoke  Run k6 async command smoke load (requires local stack)"
	@echo "  backend-perf-cleanup   Cleanup perf_* DBs (requires Postgres + BFF)"
	@echo "  backend-methods        Regenerate docs/BACKEND_METHODS.md"
	@echo "  backend-methods-check  Verify docs/BACKEND_METHODS.md is up to date"
	@echo "  frontend-check         Install + lint + build frontend"
	@echo "  ci                     Run backend-unit + frontend-check"
	@echo "  stack-up               Start full local docker stack"
	@echo "  stack-up-build         Start stack (with --build)"
	@echo "  stack-down             Stop full local docker stack"

.PHONY: backend-unit
backend-unit:
	PYTHONPATH=backend $(PYTHON) -m pytest -q -c backend/pytest.ini \
		backend/tests/unit backend/bff/tests backend/funnel/tests

.PHONY: backend-coverage
backend-coverage:
	rm -f coverage.xml || true
	PYTHONPATH=backend $(PYTHON) -m pytest -q -c backend/pytest.ini \
		backend/tests/unit backend/bff/tests backend/funnel/tests \
		--cov=backend/bff --cov=backend/oms --cov=backend/funnel --cov=backend/shared --cov=backend/instance_worker \
		--cov-branch --cov-report=term-missing:skip-covered --cov-report=xml:coverage.xml \
		--cov-fail-under=$(COVERAGE_MIN)

.PHONY: backend-prod-quick
backend-prod-quick:
	PYTHON_BIN="$(if $(findstring /,$(PYTHON)),$(abspath $(PYTHON)),$(PYTHON))" ./backend/run_production_tests.sh --quick

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

.PHONY: frontend-check
frontend-check:
	cd $(FRONTEND_DIR) && $(NPM) ci
	cd $(FRONTEND_DIR) && $(NPM) run lint
	cd $(FRONTEND_DIR) && $(NPM) run build

.PHONY: ci
ci: backend-unit frontend-check

.PHONY: stack-up
stack-up:
	$(COMPOSE) -f $(COMPOSE_FULL) up -d

.PHONY: stack-up-build
stack-up-build:
	$(COMPOSE) -f $(COMPOSE_FULL) up -d --build

.PHONY: stack-down
stack-down:
	$(COMPOSE) -f $(COMPOSE_FULL) down
