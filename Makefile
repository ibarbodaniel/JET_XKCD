.DEFAULT_GOAL := help
.RUN := poetry run
.PROJECT_NAME := xkcd_pipelines

.PHONY: help env-create env-destroy test format check

help:
	@echo "Usage: make <target>"
	@echo "Targets:"
	@echo "  env-create    Setup environment and dependencies"
	@echo "  env-destroy   Remove environment"
	@echo "  test          Run tests"
	@echo "  format        Format code"
	@echo "  check         Validate pyproject.toml"

env-create:
	poetry install --no-root -vvv

env-destroy:
	poetry env remove $(poetry env list --full-path | grep "$(.PROJECT_NAME)")

test:
	$(.RUN) pytest -v --maxfail=1 ./tests

format:
	$(.RUN) black ./$(.PROJECT_NAME) ./tests

check:
	poetry check

ci: env-create test
