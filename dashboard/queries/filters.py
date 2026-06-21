"""Construção pura de cláusulas WHERE a partir de filtros globais."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FilterState:
    anos: tuple[int, ...] = ()
    meses: tuple[int, ...] = ()
    sk_unidades: tuple[str, ...] = ()
    cd_funcoes: tuple[str, ...] = ()
    sk_naturezas_despesa: tuple[str, ...] = ()
    sk_fontes: tuple[str, ...] = ()
    sk_fornecedores: tuple[str, ...] = ()
    sk_naturezas_receita: tuple[str, ...] = ()

    def without_meses(self) -> FilterState:
        """Mesmo recorte, sem filtro de mês (ex.: último mês no ano escolhido)."""
        return FilterState(
            anos=self.anos,
            meses=(),
            sk_unidades=self.sk_unidades,
            cd_funcoes=self.cd_funcoes,
            sk_naturezas_despesa=self.sk_naturezas_despesa,
            sk_fontes=self.sk_fontes,
            sk_fornecedores=self.sk_fornecedores,
            sk_naturezas_receita=self.sk_naturezas_receita,
        )


def _in_clause(column: str, values: tuple, params: list) -> str | None:
    if not values:
        return None
    placeholders = ", ".join("?" for _ in values)
    params.extend(values)
    return f"{column} IN ({placeholders})"


def build_time_where(
    filters: FilterState,
    params: list,
    *,
    tempo_alias: str = "t",
) -> list[str]:
    clauses: list[str] = []
    if filters.anos:
        c = _in_clause(f"{tempo_alias}.nu_ano", filters.anos, params)
        if c:
            clauses.append(c)
    if filters.meses:
        c = _in_clause(f"{tempo_alias}.nu_mes", filters.meses, params)
        if c:
            clauses.append(c)
    return clauses


def build_receita_where(
    filters: FilterState,
    params: list,
    *,
    fact_alias: str = "f",
    tempo_alias: str = "t",
) -> str:
    clauses = build_time_where(filters, params, tempo_alias=tempo_alias)
    if filters.sk_naturezas_receita:
        c = _in_clause(
            f"{fact_alias}.sk_natureza_receita",
            filters.sk_naturezas_receita,
            params,
        )
        if c:
            clauses.append(c)
    if not clauses:
        return "1=1"
    return " AND ".join(clauses)


def build_despesa_where(
    filters: FilterState,
    params: list,
    *,
    fact_alias: str = "f",
    tempo_alias: str = "t",
    funcional_alias: str = "fp",
) -> str:
    clauses = build_time_where(filters, params, tempo_alias=tempo_alias)
    if filters.sk_unidades:
        c = _in_clause(
            f"{fact_alias}.sk_unidade_administrativa",
            filters.sk_unidades,
            params,
        )
        if c:
            clauses.append(c)
    if filters.cd_funcoes:
        c = _in_clause(
            funcional_cd_funcao_sql(funcional_alias),
            filters.cd_funcoes,
            params,
        )
        if c:
            clauses.append(c)
    if filters.sk_naturezas_despesa:
        c = _in_clause(
            f"{fact_alias}.sk_natureza_despesa",
            filters.sk_naturezas_despesa,
            params,
        )
        if c:
            clauses.append(c)
    if filters.sk_fontes:
        c = _in_clause(
            f"{fact_alias}.sk_fonte_recurso", filters.sk_fontes, params
        )
        if c:
            clauses.append(c)
    if filters.sk_fornecedores:
        c = _in_clause(
            f"{fact_alias}.sk_fornecedor", filters.sk_fornecedores, params
        )
        if c:
            clauses.append(c)
    if not clauses:
        return "1=1"
    return " AND ".join(clauses)


def funcional_cd_funcao_sql(funcional_alias: str = "fp") -> str:
    """Extrai código de função (2 dígitos) da classificação funcional PJF."""
    col = f"{funcional_alias}.cd_funcional_pragmatica"
    return f"""case
        when strpos({col}, '.') > 0
            then lpad(substr({col}, 1, 2), 2, '0')
        when try_cast(substr({col}, 1, 2) as integer) > 28
            and try_cast(substr({col}, 1, 2) as integer) <> 99
            then lpad(substr({col}, 1, 1), 2, '0')
        else lpad(substr({col}, 1, 2), 2, '0')
    end"""


def funcional_cd_subfuncao_sql(funcional_alias: str = "fp") -> str:
    """Extrai subfunção (3 dígitos); posição depende de código com ou sem ponto."""
    col = f"{funcional_alias}.cd_funcional_pragmatica"
    return f"""case
        when strpos({col}, '.') > 0
            then lpad(substr({col}, 4, 3), 3, '0')
        when try_cast(substr({col}, 1, 2) as integer) > 28
            and try_cast(substr({col}, 1, 2) as integer) <> 99
            then lpad(substr({col}, 2, 3), 3, '0')
        else lpad(substr({col}, 3, 3), 3, '0')
    end"""


def despesa_from_joins() -> str:
    return """
        from dwh.fct_despesa f
        join dwh.dim_tempo t on f.sk_tempo_empenho = t.sk_tempo
        left join dwh.dim_unidade_administrativa ua
            on f.sk_unidade_administrativa = ua.sk_unidade_administrativa
        left join dwh.dim_funcional_pragmatica fp
            on f.sk_funcional_pragmatica = fp.sk_funcional_pragmatica
        left join dwh.dim_natureza_despesa nd
            on f.sk_natureza_despesa = nd.sk_natureza_despesa
        left join dwh.dim_fonte_recurso fr
            on f.sk_fonte_recurso = fr.sk_fonte_recurso
        left join dwh.dim_fornecedor fo on f.sk_fornecedor = fo.sk_fornecedor
    """


def receita_from_joins() -> str:
    return """
        from dwh.fct_receita f
        join dwh.dim_tempo t on f.sk_tempo_referencia = t.sk_tempo
        join dwh.dim_natureza_receita nr
            on f.sk_natureza_receita = nr.sk_natureza_receita
    """


def receita_acumulada_from_joins() -> str:
    return """
        from dwh.fct_receita_acumulada f
        join dwh.dim_tempo t on f.sk_tempo_referencia = t.sk_tempo
        join dwh.dim_natureza_receita nr
            on f.sk_natureza_receita = nr.sk_natureza_receita
    """
