uv_venv:
	if [ ! -d ".venv" ]; then uv venv; fi

dev_install: uv_venv
	uv sync --extra dev

dg: dev_install
	uv run dg dev

dashboard: dev_install
	uv run streamlit run dashboard/app.py

dbt-%:
	uv run dbt run --project-dir execucao_orcamentaria_dbt --profiles-dir execucao_orcamentaria_dbt --select $*
	uv run dbt test --project-dir execucao_orcamentaria_dbt --profiles-dir execucao_orcamentaria_dbt --select $*