# Makefile
COMPOSE_DIR    := infrastructure
DC             := docker compose -f $(COMPOSE_DIR)/docker-compose.yml
DC_TEST        := docker compose -f docker-compose.test.yml

.PHONY: up down restart logs ps \
        kafka-reset kafka-reset-full \
        test test-unit test-integration test-coverage \
        lint format typecheck \
        db-shell db-migrate db-backup \
        drift retrain \
        clean clean-volumes

# ── Stack management ────────────────────────────────────────────────────────

up:
	$(DC) up --build

up-detach:
	$(DC) up --build -d

down:
	$(DC) down

restart:
	$(DC) restart

logs:
	$(DC) logs -f --tail=100

ps:
	$(DC) ps

# ── Kafka recovery ───────────────────────────────────────────────────────────

kafka-reset:
	powershell -ExecutionPolicy Bypass \
	  -File "$(COMPOSE_DIR)/scripts/reset-kafka-zk.ps1"

kafka-reset-full:
	powershell -ExecutionPolicy Bypass \
	  -File "$(COMPOSE_DIR)/scripts/reset-kafka-zk.ps1" -Full

# ── Testing ──────────────────────────────────────────────────────────────────

test: test-unit

test-unit:
	pip install -q -r requirements-dev.txt
	pytest tests/ -v --ignore=tests/integration

test-integration:
	$(DC_TEST) up -d
	@echo "Waiting for services..."
	@sleep 15
	PG_PORT=5433 PG_PASSWORD=nexus_test_pass \
	  pytest tests/integration/ -v
	$(DC_TEST) down -v

test-coverage:
	pip install -q -r requirements-dev.txt
	pytest tests/ --cov=. --cov-report=html --cov-report=term-missing
	@echo "Coverage report: htmlcov/index.html"

# ── Code quality ─────────────────────────────────────────────────────────────

lint:
	ruff check .

format:
	black .
	ruff check . --fix

typecheck:
	mypy common/ kafka_producer/ ml_models/ ai_copilot/ api_service/ \
	  --ignore-missing-imports

# ── Database management ───────────────────────────────────────────────────────

db-shell:
	$(DC) exec postgres psql -U nexus -d nexus

db-migrate:
	$(DC) exec postgres psql -U nexus -d nexus \
	  -c "\i /docker-entrypoint-initdb.d/migrations/apply.sql"

db-optimize:
	$(DC) exec anomaly-detector python scripts/db_maintenance.py

db-backup:
	$(DC) exec postgres pg_dump -U nexus -d nexus \
	  | gzip > backup_$$(date +%Y%m%d_%H%M%S).sql.gz
	@echo "Backup complete"

db-partition-check:
	$(DC) exec postgres psql -U nexus -d nexus -c \
	  "SELECT tablename, pg_size_pretty(pg_total_relation_size(tablename::text)) \
	   FROM pg_tables WHERE tablename LIKE 'order_events_%' ORDER BY 1;"

# ── ML pipeline ───────────────────────────────────────────────────────────────

drift:
	$(DC) exec anomaly-detector python drift_monitor.py

retrain:
	$(DC) exec anomaly-detector python retrain_production_model.py --days 30

retrain-full:
	$(DC) exec anomaly-detector python retrain_production_model.py --days 90

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf htmlcov/ .coverage .pytest_cache/

clean-volumes:
	$(DC) down -v
	@echo "All Docker volumes removed"

# ── Validation ────────────────────────────────────────────────────────────────

validate:
	python scripts/validate_system.py

health:
	@curl -s http://localhost:8000/health | python -m json.tool
	@echo ""
	@curl -s http://localhost:8501/_stcore/health
