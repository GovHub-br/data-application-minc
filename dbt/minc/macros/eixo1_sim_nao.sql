{#-
  A regra "sim ou não" dos scripts do Eixo 1 do Cultura em Números (o fix_sim_nao
  do MUNIC.R): a resposta que contém "sim", ou é "1" ou "s", vira 'Sim'; qualquer
  outra resposta preenchida, inclusive '-', 'Recusa' e 'Não informou', vira 'Não'.

  Uma diferença do R, pela regra transversal 1 do ADR 0010: resposta ausente fica
  NULL. O R a transformava em 'Não'.

  `~*` e não lower(): o lower() do Postgres depende do locale do banco para letra
  acentuada, e o padrão aqui é todo ASCII.
-#}
{% macro eixo1_sim_nao(col) -%}
    case
        when {{ col }} is null
        then null
        when {{ col }} ~* 'sim|^1$|^s$'
        then 'Sim'
        else 'Não'
    end
{%- endmacro %}
