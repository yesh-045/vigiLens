.PHONY: help install install-test install-docs docs-serve docs-build \
       test test-unit test-integration test-contract \
       test-cov test-cov-html lint \
       up up-external down down-external logs build \
	api stream-worker llm-worker scene-worker \
       deploy-reranker deploy-reranker-stop \
       rtsp-stream rtsp-stream-mac \
       db-init db-generate-key clean

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
COMPOSE          := docker compose
COMPOSE_EXT      := docker compose -f docker-compose.external.yml
PYTHON           := python
PYTEST           := uv run --active pytest
COV_MIN          := 45

# ──────────────────────────────────────────────────────────────────────────────
# Help
# ──────────────────────────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ──────────────────────────────────────────────────────────────────────────────
# Dependencies
# ──────────────────────────────────────────────────────────────────────────────
install: ## Install project dependencies
	pip install -e .

install-test: ## Install test dependencies
	pip install -e ".[test]"

install-docs: ## Install docs dependencies
	npm install

docs-serve: ## Serve docs locally
	npm run start

docs-build: ## Build docs site
	npm run build

# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────
test: ## Run the full test suite
	$(PYTEST) tests/ -v

test-unit: ## Run unit tests only
	$(PYTEST) tests/unit/ -v

test-integration: ## Run integration tests only
	$(PYTEST) tests/integration/ -v

test-contract: ## Run contract tests only
	$(PYTEST) tests/contract/ -v

test-cov: ## Run tests with coverage report (terminal)
	$(PYTEST) tests/ --cov=vigilens --cov-report=term-missing --cov-fail-under=$(COV_MIN)

test-cov-html: ## Run tests with HTML coverage report
	$(PYTEST) tests/ --cov=vigilens --cov-report=html --cov-report=term-missing
	@echo "\n  Open htmlcov/index.html in your browser"

test-fast: ## Run tests excluding slow markers
	$(PYTEST) tests/ -v -x -q

# ──────────────────────────────────────────────────────────────────────────────
# Lint / Format
# ──────────────────────────────────────────────────────────────────────────────
lint: ## Run ruff linter
	$(PYTHON) -m ruff check vigilens/ tests/

format: ## Auto-format code with ruff
	$(PYTHON) -m ruff format vigilens/ tests/
	$(PYTHON) -m ruff check --fix vigilens/ tests/

# ──────────────────────────────────────────────────────────────────────────────
# Docker Compose — full stack (Redis + MinIO + app services)
# ──────────────────────────────────────────────────────────────────────────────
up: ## Start full stack (Redis + MinIO + API + workers)
	$(COMPOSE) up -d --build

down: ## Stop full stack and remove containers
	$(COMPOSE) down

up-detach: ## Start full stack in detached mode (no build)
	$(COMPOSE) up -d

logs: ## Tail logs from all services
	$(COMPOSE) logs -f

logs-api: ## Tail API service logs
	$(COMPOSE) logs -f api

logs-stream: ## Tail stream worker logs
	$(COMPOSE) logs -f stream_worker

logs-llm: ## Tail LLM worker logs
	$(COMPOSE) logs -f llm_worker

logs-scene: ## Tail scene worker logs
	$(COMPOSE) logs -f scene_worker

build: ## Build Docker images without starting
	$(COMPOSE) build

ps: ## Show running containers
	$(COMPOSE) ps

# ──────────────────────────────────────────────────────────────────────────────
# Docker Compose — external stack (app services only, external Redis/MinIO)
# ──────────────────────────────────────────────────────────────────────────────
up-external: ## Start external stack (API + workers, uses external Redis/MinIO)
	$(COMPOSE_EXT) up -d --build

down-external: ## Stop external stack
	$(COMPOSE_EXT) down

logs-external: ## Tail logs from external stack
	$(COMPOSE_EXT) logs -f

# ──────────────────────────────────────────────────────────────────────────────
# Run services locally (no Docker)
# ──────────────────────────────────────────────────────────────────────────────
api: ## Run API server locally (uvicorn with reload)
	uvicorn vigilens.apps.api.app:app --host 0.0.0.0 --port 8000 --reload

stream-worker: ## Run stream worker locally
	$(PYTHON) -m vigilens.apps.workers.stream.worker

llm-worker: ## Run LLM worker locally
	$(PYTHON) -m vigilens.apps.workers.llm.worker

scene-worker: ## Run scene worker locally
	$(PYTHON) -m vigilens.apps.workers.scene.worker

# ──────────────────────────────────────────────────────────────────────────────
# Modal deployment (reranker service)
# ──────────────────────────────────────────────────────────────────────────────
deploy-reranker: ## Deploy reranker service to Modal
	modal deploy vigilens/reranker/service.py

deploy-reranker-stop: ## Stop the reranker service on Modal
	modal app stop reranker-service

deploy-reranker-logs: ## Stream reranker logs from Modal
	modal app logs reranker-service

deploy-reranker-shell: ## Open a shell in the running reranker container
	modal shell vigilens/reranker/service.py

# ──────────────────────────────────────────────────────────────────────────────
# RTSP test stream
# ──────────────────────────────────────────────────────────────────────────────
rtsp-stream-mac: ## Start a test RTSP stream on macOS (uses mediamtx + ffmpeg)
	bash scripts/start_rtsp_stream_mac.sh

rtsp-stream: ## Start a test RTSP stream on Linux
	bash scripts/start_rtsp_stream.sh

# ──────────────────────────────────────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────────────────────────────────────
db-init: ## Create database tables (runs init_db)
	$(PYTHON) -c "from vigilens.core.db import init_db; init_db()"

db-generate-key: ## Generate a new API key for a tenant (TENANT_ID=xxx)
	@test -n "$(TENANT_ID)" || (echo "Usage: make db-generate-key TENANT_ID=xxx"; exit 1)
	$(PYTHON) -c "\
	import asyncio; \
	from vigilens.auth.keys import generate_api_key, hash_api_key, key_prefix; \
	from vigilens.db.engine import init_db; \
	from vigilens.db.repository import create_api_key; \
	async def main(): \
	    await init_db(); \
	    raw = generate_api_key(); \
	    await create_api_key(tenant_id='$(TENANT_ID)', key_hash=hash_api_key(raw), key_prefix=key_prefix(raw), name='cli-generated'); \
	    print(f'API Key (save this — shown once): {raw}'); \
	asyncio.run(main())"

# ──────────────────────────────────────────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────────────────────────────────────────
clean: ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/ .ruff_cache/ *.egg-info/ dist/ build/
	@echo "  Cleaned."

clean-docker: ## Remove Docker volumes and images for this project
	$(COMPOSE) down -v --rmi local
	@echo "  Docker resources cleaned."
