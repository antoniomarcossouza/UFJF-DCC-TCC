"""Queries para popular opções de filtros na sidebar."""

from __future__ import annotations

from dashboard.queries.filters import funcional_cd_funcao_sql


def anos_disponiveis() -> tuple[str, list]:
    return (
        """
        select distinct nu_ano as valor, cast(nu_ano as varchar) as rotulo
        from dwh.dim_tempo t
        where exists (
            select 1 from dwh.fct_receita f
            where f.sk_tempo_referencia = t.sk_tempo
        )
        or exists (
            select 1 from dwh.fct_despesa f
            where f.sk_tempo_empenho = t.sk_tempo
        )
        order by nu_ano
        """,
        [],
    )


def _meses_com_dados_sql(ano_filter: str) -> str:
    """Meses com movimento em despesa ou arrecadação (evita opções vazias)."""
    return f"""
        select distinct t.nu_mes as valor,
            t.nm_mes || ' (' || cast(t.nu_mes as varchar) || ')' as rotulo
        from dwh.dim_tempo t
        where {ano_filter}
          and (
            exists (
                select 1 from dwh.fct_despesa f
                where f.sk_tempo_empenho = t.sk_tempo
            )
            or exists (
                select 1 from dwh.fct_receita f
                where f.sk_tempo_referencia = t.sk_tempo
                  and coalesce(f.vl_arrecadada_mes, 0) <> 0
            )
          )
        order by t.nu_mes
    """


def meses_disponiveis(anos: tuple[int, ...]) -> tuple[str, list]:
    if not anos:
        return (_meses_com_dados_sql("1=1"), [])
    placeholders = ", ".join("?" for _ in anos)
    return (
        _meses_com_dados_sql(f"t.nu_ano in ({placeholders})"),
        list(anos),
    )


def unidades_administrativas() -> tuple[str, list]:
    return (
        """
        select sk_unidade_administrativa as valor,
            nm_unidade_administrativa as rotulo
        from dwh.dim_unidade_administrativa
        order by rotulo
        """,
        [],
    )


def funcoes() -> tuple[str, list]:
    cd_funcao = funcional_cd_funcao_sql("fp")
    return (
        f"""
        select distinct
            {cd_funcao} as valor,
            {cd_funcao} || ' - ' || coalesce(fn.ds_funcao, 'Função ' || {cd_funcao}) as rotulo
        from dwh.dim_funcional_pragmatica fp
        left join dwh.dim_funcao fn on {cd_funcao} = fn.cd_funcao
        where fp.cd_funcional_pragmatica is not null
        order by valor
        """,
        [],
    )


def naturezas_despesa() -> tuple[str, list]:
    return (
        """
        select sk_natureza_despesa as valor,
            cd_natureza_despesa || ' - ' || ds_natureza_despesa as rotulo
        from dwh.dim_natureza_despesa
        order by rotulo
        """,
        [],
    )


def fontes_recurso() -> tuple[str, list]:
    return (
        """
        select sk_fonte_recurso as valor,
            cd_fonte_recurso || ' - ' || ds_fonte_recurso as rotulo
        from dwh.dim_fonte_recurso
        order by rotulo
        """,
        [],
    )


def fornecedores() -> tuple[str, list]:
    return (
        """
        select sk_fornecedor as valor,
            coalesce(nm_fornecedor, cd_cpf_cnpj) as rotulo
        from dwh.dim_fornecedor
        order by rotulo
        """,
        [],
    )


def naturezas_receita() -> tuple[str, list]:
    return (
        """
        select sk_natureza_receita as valor,
            cd_natureza_receita || ' - ' || ds_natureza_receita as rotulo
        from dwh.dim_natureza_receita
        order by rotulo
        """,
        [],
    )
