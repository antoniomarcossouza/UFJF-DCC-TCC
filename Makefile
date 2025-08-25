uv_install:
	pip install uv

uv_venv:
	if [ ! -d ".venv" ]; then uv venv; fi

dev_install: uv_install uv_venv
	uv sync --extra dev

dev: dev_install
	uv run dg dev

ruff: dev_install
	uv run --extra dev ruff check .