# Urban traffic accident risk analysis - developer entry points.
#
# Every target is a one-liner around docker compose, so the project runs the
# same way on any machine with Docker: no local Python setup required.
SHELL := /bin/bash
COMPOSE := docker compose
PROCESSING := $(COMPOSE) run --rm processing
LOGICAL_DATE ?= 2024-03-01
TASK ?= run_analysis

.DEFAULT_GOAL := help
.PHONY: help env build up down restart ps logs psql fixture run extract anonymize \
        load analyze visualize verify test test-unit test-integration dag-test \
        versions charts clean nuke

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from .env.example with freshly generated secrets
	@test -f .env || { \
		cp .env.example .env; \
		fernet=$$(openssl rand -base64 32 | tr '+/' '-_'); \
		sed -i.bak \
			-e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$fernet|" \
			-e "s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$$(openssl rand -hex 32)|" \
			-e "s|^PSEUDONYMIZATION_PEPPER=.*|PSEUDONYMIZATION_PEPPER=$$(openssl rand -hex 32)|" \
			.env; \
		rm -f .env.bak; \
	}
	@echo ".env ready"

build: env ## Build the processing and Airflow images
	$(COMPOSE) build

up: env ## Start postgres, airflow and processing
	$(COMPOSE) up -d --build
	@echo "Airflow UI: http://localhost:8080"

down: ## Stop the containers (keeps the database volume)
	$(COMPOSE) down

restart: down up ## Recreate the containers

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Follow the logs of all containers
	$(COMPOSE) logs -f --tail=100

psql: ## Open a psql shell on the warehouse database
	$(COMPOSE) exec postgres psql -U traffic -d traffic

fixture: ## Regenerate the offline demo fixture (90k rows, seeded)
	docker run --rm -v "$(PWD)":/w -w /w python:3.11-slim python tools/make_fixture.py

run: ## Run all five pipeline steps for LOGICAL_DATE
	$(PROCESSING) python -m accident_risk.cli run --date $(LOGICAL_DATE)

extract: ## Task 1 only
	$(PROCESSING) python -m accident_risk.cli extract --date $(LOGICAL_DATE)

anonymize: ## Task 2 only
	$(PROCESSING) python -m accident_risk.cli anonymize --date $(LOGICAL_DATE)

load: ## Task 3 only
	$(PROCESSING) python -m accident_risk.cli load --date $(LOGICAL_DATE)

analyze: ## Task 4 only
	$(PROCESSING) python -m accident_risk.cli analyze --date $(LOGICAL_DATE)

visualize: ## Task 5 only
	$(PROCESSING) python -m accident_risk.cli visualize --date $(LOGICAL_DATE)

verify: ## Run the GDPR and integrity checks against the warehouse
	$(PROCESSING) python -m accident_risk.cli verify

test: ## Run the whole test suite (unit + integration)
	$(PROCESSING) python -m pytest -q

test-unit: ## Run only the tests that need no database
	$(PROCESSING) python -m pytest -q -m "not integration"

test-integration: ## Run only the database-backed tests
	$(PROCESSING) python -m pytest -q -m integration

dag-test: ## Run one DAG task (per-task validation; `dags test` is broken with a Deadline Alert on 3.4.0)
	$(COMPOSE) exec -T airflow airflow tasks test urban_accident_risk $(TASK) $(LOGICAL_DATE)

versions: ## Print library versions in both app containers (drift check)
	@for svc in processing airflow; do \
		printf '%-10s ' "$$svc:"; \
		$(COMPOSE) exec -T $$svc python -c 'import sys, pandas, numpy, matplotlib; \
			print("python", sys.version.split()[0], "| pandas", pandas.__version__, \
			"| numpy", numpy.__version__, "| matplotlib", matplotlib.__version__)'; \
	done

charts: ## Show the generated chart files
	@ls -lh outputs/$(LOGICAL_DATE)/*.png

clean: ## Remove generated artifacts (keeps the database and the fixture)
	rm -rf outputs/* data/staging/* data/raw/* .pytest_cache
	@touch data/raw/.gitkeep data/staging/.gitkeep outputs/.gitkeep

nuke: down ## Stop everything and delete the database volume
	docker volume rm urban-traffic-accident-risk_pgdata || true
