.PHONY: install lint test ci ingest status

install:          ## venv, dépendances, outils, hooks git
	uv sync --all-groups
	uv run pre-commit install

lint:
	uv run ruff check . && uv run ruff format --check .

test:
	uv run pytest

ci:               ## ce que GitHub Actions exécute
	$(MAKE) lint && $(MAKE) test

ingest:           ## make ingest source=fake|entsoe|esios
	uv run straitflow ingest --source $(or $(source),fake)

status:
	uv run straitflow status
