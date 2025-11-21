.PHONY: help up down restart install clean logs status airflow mlflow minio

help:
	@echo "Available commands:"
	@echo "  make up        - Start all services"
	@echo "  make down      - Stop all services"
	@echo "  make restart   - Restart all services"
	@echo "  make install   - Rebuild containers with new dependencies"
	@echo "  make clean     - Stop and remove all containers, networks, and volumes"
	@echo "  make logs      - Follow logs from all services"
	@echo "  make status    - Show status of all services"
	@echo ""
	@echo "Open web interfaces:"
	@echo "  make airflow   - Open Airflow UI (http://localhost:8080)"
	@echo "  make mlflow    - Open MLflow UI (http://localhost:5001)"
	@echo "  make minio     - Open MinIO UI (http://localhost:9001)"

up:
	docker-compose --profile all up -d

down:
	docker-compose --profile all down

restart:
	docker-compose --profile all restart

install:
	docker-compose build airflow-apiserver airflow-scheduler airflow-worker airflow-dag-processor
	docker-compose --profile all restart

clean:
	docker-compose --profile all down -v
	@echo "All containers, networks, and volumes removed"

logs:
	docker-compose --profile all logs -f

status:
	docker-compose --profile all ps

airflow:
	@echo "Opening Airflow UI..."
	@open http://localhost:8080 || xdg-open http://localhost:8080 2>/dev/null || echo "Please open http://localhost:8080 in your browser"

mlflow:
	@echo "Opening MLflow UI..."
	@open http://localhost:5001 || xdg-open http://localhost:5001 2>/dev/null || echo "Please open http://localhost:5001 in your browser"

minio:
	@echo "Opening MinIO UI..."
	@open http://localhost:9001 || xdg-open http://localhost:9001 2>/dev/null || echo "Please open http://localhost:9001 in your browser"
