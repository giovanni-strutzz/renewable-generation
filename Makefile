# ==============================================================================
# PROJECT COMMANDS
# ==============================================================================

# Variables
# Detects if it should use 'docker compose' (V2) or 'docker-compose' (V1)
DOCKER_COMPOSE := $(shell which docker-compose 2>/dev/null || echo "docker compose")
CONTAINER_NAME := airflow-scheduler

.PHONY: help build up down restart logs ps fix-perm clean test-unit test-int test

help:
	@echo "========================================================================"
	@echo "RENEWABLE ETL PIPELINE - MAKEFILE"
	@echo "========================================================================"
	@echo "Infra Commands:"
	@echo "  make build          - Build custom images (Spark + Airflow)"
	@echo "  make up             - Start all services in detached mode"
	@echo "  make down           - Stop and remove all containers"
	@echo "  make restart        - Restart all services"
	@echo "  make clean          - Stop services and delete all volumes (Wipes Data)"
	@echo ""
	@echo "Utility Commands:"
	@echo "  make logs           - Follow logs from the Airflow webserver"
	@echo "  make ps             - List all running project containers"
	@echo "  make fix-perm       - Grant read/write permissions (Linux)"
	@echo ""
	@echo "Test Commands:"
	@echo "  make test-unit      - Run Unit Tests (Fast, no infra needed)"
	@echo "  make test-int       - Run Integration Tests (Requires S3/Mongo active)"
	@echo "  make test           - Run all tests"
	@echo "========================================================================"

# --- Infrastructure ---

build:
	$(DOCKER_COMPOSE) build

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

restart:
	$(DOCKER_COMPOSE) restart

clean:
	$(DOCKER_COMPOSE) down -v

# --- Utilities ---

logs:
	docker logs -f airflow-webserver

ps:
	docker ps

fix-perm:
	sudo chmod -R 777 ./dags ./logs ./plugins ./src/jobs

# --- Testing ---
# Helper to ensure test dependencies are installed inside the container
_install-test-deps:
	@docker exec -u 0 $(CONTAINER_NAME) pip install -q pytest pytest-mock pymongo requests

test-unit: _install-test-deps
	docker exec -it $(CONTAINER_NAME) pytest tests/test_unit.py

test-int: _install-test-deps
	docker exec -it $(CONTAINER_NAME) pytest tests/test_integration.py

test: _install-test-deps
	docker exec -it $(CONTAINER_NAME) pytest tests/