# STN - Ementario de Natureza de Receita

## Fontes

- Pagina atual (2024+):
  - https://www.gov.br/tesouronacional/pt-br/contabilidade-e-custos/federacao/ementario-da-classificacao-por-natureza-de-receita-tabela-de-codigos
- Pagina de edicoes anteriores (2014-2023):
  - https://www.gov.br/tesouronacional/pt-br/contabilidade-e-custos/federacao/edicoes-anteriores-ementario-da-receita-orcamentaria/edicao_anterior_ementario-da-classificacao-por-natureza-de-receita-tabela-de-codigos

## Regra de descoberta de arquivo por ano

ETL busca no HTML ancora com texto:

`Ementario - Tabela de Codigos - valido para {ano}`

e extrai `href` correspondente. Link pode apontar para:

- `thot-arquivos.tesouro.gov.br/publicacao-anexo/{id}`
- `sisweb.tesouro.gov.br/apex/...`

Ambos redirecionam para arquivo final `.xlsx` no CDN do Tesouro.

## Observacao importante

Somente **receitas** tem ementario STN unificado nesta implementacao (XLS anual via Dagster).

Para **classificacao funcional** (funcao e subfuncao), a referencia adotada e a
**LOA PJF** — ver [`references/LOA_PJF.md`](LOA_PJF.md) e asset Dagster
`pjf_loa_funcao` / `pjf_loa_subfuncao`.

Natureza de despesa vem das descricoes publicadas na execucao mensal PJF
(`dim_natureza_despesa`).
