# Dashboard — Execução Orçamentária Municipal

Dashboard analítico Streamlit sobre o warehouse DuckDB (`data/execucao_orcamentaria.duckdb`, schema `dwh`).

## Pré-requisitos

- Python 3.13+
- Warehouse materializado via dbt (`execucao_orcamentaria_dbt`)
- Arquivo `data/execucao_orcamentaria.duckdb` presente

## Instalação

Na raiz do repositório (dependências no `pyproject.toml`):

```bash
uv sync --extra dev
```

## Execução

```bash
# na raiz do repo
make dashboard
```

Ou:

```bash
uv run streamlit run dashboard/app.py
```

Variável opcional:

```bash
set DUCKDB_PATH=C:\caminho\execucao_orcamentaria.duckdb
```

## Páginas

| Página | Conteúdo |
|--------|----------|
| Visão Geral Fiscal | Saldo, arrecadação vs pagamentos |
| Receitas | Execução, sazonalidade, deduções |
| Despesas | Empenho, liquidação, pagamento |
| Fornecedores | Ranking e concentração |
| Execução Orçamentária | Unidades e funções |
| Indicadores Per Capita | Métricas por habitante |
| Exploração Detalhada | Tabelas exportáveis |

## Filtros globais

Ano, mês, unidade administrativa, função, natureza despesa, fonte, fornecedor, natureza receita.

## População (per capita)

Editar `dashboard/config/populacao.toml`:

```toml
[populacao]
2026 = 75000
```

Se valor for 0 ou ausente, KPIs per capita ficam ocultos com aviso.

## Testes

```bash
uv run pytest dashboard/tests -q
```

## Limitações conhecidas

- **Orçamento autorizado** não existe no modelo — não há % sobre dotação.
- **Despesa**: cobertura depende dos dados ingeridos (atualmente pode haver só um mês).
- **Função/subfunção**: códigos numéricos + descrição da ação; sem nomes MCASP.
- **População**: configuração manual; não há integração IBGE automática.

## Estrutura

```
dashboard/
├── app.py
├── pages/
├── queries/      # SQL builders (functional core)
├── components/   # UI Streamlit
├── utils/        # IO DuckDB (shell)
├── config/
└── tests/
```
