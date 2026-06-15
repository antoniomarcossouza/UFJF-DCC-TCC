"""Queries para popular opções de filtros na sidebar."""

from __future__ import annotations


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


def meses_disponiveis(anos: tuple[int, ...]) -> tuple[str, list]:
    if not anos:
        return (
            """
            select distinct nu_mes as valor,
                nm_mes || ' (' || cast(nu_mes as varchar) || ')' as rotulo
            from dwh.dim_tempo
            order by nu_mes
            """,
            [],
        )
    placeholders = ", ".join("?" for _ in anos)
    return (
        f"""
        select distinct nu_mes as valor,
            nm_mes || ' (' || cast(nu_mes as varchar) || ')' as rotulo
        from dwh.dim_tempo
        where nu_ano in ({placeholders})
        order by nu_mes
        """,
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
    return (
        """
        select distinct
            substr(cd_funcional_pragmatica, 1, 2) as valor,
            substr(cd_funcional_pragmatica, 1, 2) as rotulo
        from dwh.dim_funcional_pragmatica
        where cd_funcional_pragmatica is not null
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
