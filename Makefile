COMPOSE_DIR := infrastructure
DC := docker compose -f $(COMPOSE_DIR)/docker-compose.yml

.PHONY: up down kafka-reset kafka-reset-full

up:
	$(DC) up --build

down:
	$(DC) down

kafka-reset:
	powershell -ExecutionPolicy Bypass -File "$(COMPOSE_DIR)/scripts/reset-kafka-zk.ps1"

kafka-reset-full:
	powershell -ExecutionPolicy Bypass -File "$(COMPOSE_DIR)/scripts/reset-kafka-zk.ps1" -Full
