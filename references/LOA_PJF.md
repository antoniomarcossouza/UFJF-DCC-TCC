# LOA PJF - Classificação funcional (DimLOA)

## Fonte

- **Zip anual:** `https://www.pjf.mg.gov.br/transparencia/orcamento/loa/{ano}/arquivos/pdf/Funcionais.zip`
- **Referência local:** [`references/LOA_PJF/Funcionais/`](LOA_PJF/Funcionais/)

O zip contém PDFs DimLOA. A ingestão v1 usa:

| PDF | Conteúdo | Registros (2026) |
|-----|----------|------------------|
| `Funcao.pdf` | Funções (01–28 + 99) | 29 |
| `SubFuncao.pdf` | Subfunções (lista plana) | 117 |

Função e subfunção são **independentes** na LOA PJF (combinação livre na execução).

## Pipeline

| Camada | Artefato |
|--------|----------|
| Dagster | `pjf_loa_funcao`, `pjf_loa_subfuncao` (sem partição; exercício fixo **2026** em `LOA_YEAR`) |
| Raw FS | `data/pjf_loa_funcionais/{ano}.zip` |
| DuckDB | `stg.pjf_loa_funcao`, `stg.pjf_loa_subfuncao` |
| Staging | `stg_pjf_loa_funcao`, `stg_pjf_loa_subfuncao` |
| Warehouse | `dim_funcao`, `dim_subfuncao` |

## Dependências de sistema

- `poppler-utils` - `pdftotext`, `pdftoppm`
- `tesseract-ocr` + `tesseract-ocr-por` - OCR dos PDFs escaneados

Parser: [`execucao_orcamentaria/parsers/loa_funcionais.py`](../execucao_orcamentaria/parsers/loa_funcionais.py)

## Materialização

Reconstrua a imagem após mudanças no Dockerfile (`poppler` + `tesseract`):

```bash
docker compose build ufjf_tcc_user_code
docker compose up -d ufjf_tcc_user_code ufjf_tcc_daemon ufjf_tcc_webserver
```

Materialize os assets (função baixa o zip; subfunção depende dela):

```bash
dg asset materialize -m execucao_orcamentaria.defs.pjf.assets.loa_funcionais --select pjf_loa_funcao,pjf_loa_subfuncao

dbt build --select stg_pjf_loa_funcao+ stg_pjf_loa_subfuncao+
```

## Atualização anual

1. Confirmar URL do zip no portal de transparência da PJF.
2. Validar/adaptar o parser ao layout dos PDFs do novo exercício (cada ano pode diferir).
3. Atualizar `LOA_YEAR` em `execucao_orcamentaria/defs/pjf/assets/loa_funcionais.py`.
4. Materializar os assets no Dagster e rodar `dbt build` nos modelos LOA.

## Limitações

- PDFs DimLOA são imagens; qualidade do OCR depende do Tesseract.
- Códigos ausentes na LOA do ano corrente exibem fallback no dashboard (`Função XX` / `Subfunção XXX`).
