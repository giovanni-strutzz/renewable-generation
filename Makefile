# ==============================================================================
# PROJECT COMMANDS
# ==============================================================================

# Variables
COMPOSE_FILE=docker-compose.yml

.PHONY: help build up down restart logs ps fix-perm clean

help:
	@echo "Available commands:"
	@echo "  make build       - Build the custom Airflow image (includes Spark & Java)"
	@echo "  make up          - Start all containers in detached mode"
	@echo "  make down        - Stop and remove all containers"
	@echo "  make restart     - Restart all services"
	@echo "  make logs        - Follow logs from the Airflow webserver"
	@echo "  make ps          - List all running project containers"
	@echo "  make fix-perm    - Grant read/write permissions to project folders (Linux)"
	@echo "  make clean       - Stop services and delete all volumes (Wipes Database/S3)"

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker logs -f airflow-webserver

ps:
	docker ps

fix-perm:
	sudo chmod -R 777 ./dags ./logs ./plugins ./src/jobs

clean:
	docker-compose down -v