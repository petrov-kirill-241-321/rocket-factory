SHELL := /bin/sh
COMPOSE := docker compose
DB_URL := postgres://rocket:rocket@localhost:5432/rocket_factory?sslmode=disable

.PHONY: help env up up-observability down clean logs ps \
        test test-integration cover lint vet fmt tidy proto \
        migrate-up migrate-down topics smoke

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

env: ## Создать локальный .env
	cp -n .env.example .env || true

up: ## Поднять систему (сборка идёт внутри Docker)
	$(COMPOSE) up --build

up-observability: ## Поднять систему вместе с Prometheus, Grafana и Jaeger
	OTEL_ENABLED=true $(COMPOSE) --profile observability up --build

down: ## Остановить систему
	$(COMPOSE) down

clean: ## Остановить систему и удалить данные
	$(COMPOSE) down -v --remove-orphans

logs: ## Логи всех сервисов
	$(COMPOSE) logs -f

ps: ## Состояние контейнеров
	$(COMPOSE) ps

test: ## Юнит-тесты с детектором гонок
	go test ./... -race -count=1

test-integration: ## Тесты репозиториев (нужен поднятый PostgreSQL)
	TEST_DATABASE_URL="$(DB_URL)" go test -tags=integration ./... -count=1

cover: ## Покрытие тестами
	go test ./... -race -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

vet: ## go vet
	go vet ./...

fmt: ## Форматирование
	gofmt -w -s .

lint: ## golangci-lint (устанавливается отдельно)
	golangci-lint run ./...

tidy: ## Привести go.mod и go.sum в порядок
	go mod tidy

proto: ## Сгенерировать код из proto
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/*.proto

migrate-up: ## Применить миграции
	$(COMPOSE) run --rm migrate

migrate-down: ## Откатить миграции
	$(COMPOSE) run --rm migrate -path=/migrations -database="postgres://rocket:rocket@postgres:5432/rocket_factory?sslmode=disable" down

topics: ## Показать топики Kafka
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

smoke: ## Прогнать сквозной сценарий заказа
	./scripts/smoke.sh
