{#-
  Chave alinhada a dim_tempo: md5 de strftime(data, '%Y-%m-%d') via dbt_utils.generate_surrogate_key.
  Use a mesma expressão SQL do modelo (strftime(...)) em fatos/ints.
-#}
{% macro sk_tempo_dia_data_expr(data_sql_expr) -%}
{{ dbt_utils.generate_surrogate_key(["strftime(" ~ data_sql_expr ~ ", '%Y-%m-%d')"]) }}
{%- endmacro %}
