export TAXIFLOW_DATA ?= $(CURDIR)/data
DBT = cd dbt && uv run dbt
BROKER ?= file

.PHONY: setup sample batch stream transform demo test lint dashboard up down reset

setup:        ## install deps (Python 3.12 via uv)
	uv sync --all-extras

sample:       ## deterministic offline fixture in TLC schema
	uv run taxiflow sample

batch: sample ## ingest raw files (idempotent)
	uv run taxiflow ingest

stream:       ## replay trips through the broker and consume them
	uv run taxiflow produce --broker $(BROKER)
	uv run taxiflow consume --broker $(BROKER)

transform:    ## dbt seed + run + test
	$(DBT) build --profiles-dir .

demo: batch stream transform  ## full offline pipeline
	@echo "Done. Run 'make dashboard' to explore."

dashboard:
	uv run streamlit run dashboard/app.py

test:
	uv run pytest -q

lint:
	uv run ruff check src tests dashboard scripts

up:
	docker compose up -d --wait

down:
	docker compose down

reset:        ## DESTRUCTIVE: delete local data and broker volumes
	docker compose down -v 2>/dev/null || true
	rm -rf data dbt/target
