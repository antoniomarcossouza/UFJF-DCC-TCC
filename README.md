# Data Warehouse para execução orçamentária municipal

Pipeline de dados e painel da execução orçamentária de Juiz de Fora, com extração via Dagster, modelagem dimensional em dbt/DuckDB e visualização em Streamlit.

Instância pública do painel: [juiz-de-fora-transparencia.streamlit.app](https://juiz-de-fora-transparencia.streamlit.app/)

## Configuração do ambiente

Na raiz do repositório:

```bash
make dev_install
```

## Reprodução local

### 1. Painel Streamlit

Com o ambiente instalado e o arquivo DuckDB presente:

```bash
make dashboard
```

O Streamlit sobe em [http://localhost:8501](http://localhost:8501).

### 2. Pipeline Dagster

Para reexecutar a carga a partir das fontes públicas:

```bash
make dg
```
A UI do Dagster fica em [http://localhost:3000](http://localhost:3000).

### 3. Stack Docker
Alternativa ao `make dg`:

```bash
mkdir -p data
docker compose up --build -d
```
A UI do Dagster fica em [http://localhost:3000](http://localhost:3000).