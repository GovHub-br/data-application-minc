{% macro distribuicao_cotas(
    programa,
    incluir_territorio=false,
    base="eixo2_meta3_pagamento_elegivel",
    coluna_programa="nome_programa",
    coluna_ano="ano_final",
    coluna_valor="valor_pago_num",
    coluna_documento="identificador_unico"
) %}
    {#-
  Distribuição de cotas por ano p/ UM programa (LPG ou PNAB), valor ponderado.
  Totais do ano via CTE sobre o fato filtrado (NÃO window no explodido — evita
  multiplicar por sobreposição de cotas). Reporta pct sobre total pago E sobre
  total-com-perfil; veredito vs meta.

  Args:
    programa: 'LPG' ou 'PNAB' — filtra a base pela coluna_programa.
    incluir_territorio: só LPG tem flag_territorio_vulneravel (agente c/ cidade/uf);
      no PNAB o flag é sempre NULL, então a 4ª cota (20%) só faz sentido p/ LPG.
    base: o modelo de 1 linha por pagamento que a macro lê. Precisa ter
      tem_perfil, flag_negra, flag_indigena, flag_pcd e flag_territorio_vulneravel,
      além das quatro colunas abaixo. O padrão é eixo2_meta3_pagamento_elegivel,
      o valor pelas listas de contemplados;
      eixo2_meta3_pagamento_bbagil_perfil é o valor pago pelo BB Ágil.
    coluna_programa, coluna_ano, coluna_valor, coluna_documento: os nomes, na
      base, do programa, do ano, do valor e do documento do agente. A coluna do
      ano sai na tabela final com o mesmo nome.
  Os padrões reproduzem as golds eixo2_meta3_distribuicao_cotas_{lpg,pnab}.
-#}
    with
        fct as (
            select * from {{ ref(base) }} where {{ coluna_programa }} = '{{ programa }}'
        ),
        totais as (
            select
                {{ coluna_ano }},
                sum({{ coluna_valor }}) as valor_total_ano,
                sum({{ coluna_valor }}) filter (
                    where tem_perfil
                ) as valor_total_com_perfil_ano
            from fct
            group by {{ coluna_ano }}
        ),
        cotas as (
            select
                {{ coluna_ano }},
                'negra' as grupo,
                0.25 as meta_minima,
                sum({{ coluna_valor }}) filter (where flag_negra) as valor_grupo,
                count(distinct {{ coluna_documento }}) filter (
                    where flag_negra
                ) as qtd_agentes_grupo
            from fct
            group by {{ coluna_ano }}
            union all
            select
                {{ coluna_ano }},
                'indigena',
                0.10,
                sum({{ coluna_valor }}) filter (where flag_indigena),
                count(distinct {{ coluna_documento }}) filter (where flag_indigena)
            from fct
            group by {{ coluna_ano }}
            union all
            select
                {{ coluna_ano }},
                'pcd',
                0.05,
                sum({{ coluna_valor }}) filter (where flag_pcd),
                count(distinct {{ coluna_documento }}) filter (where flag_pcd)
            from fct
            group by {{ coluna_ano }}
            {%- if incluir_territorio %}
                union all
                -- 4ª cota: território vulnerabilizado (periférico), meta 20%. Só LPG.
                select
                    {{ coluna_ano }},
                    'territorio_vulneravel',
                    0.20,
                    sum({{ coluna_valor }}) filter (where flag_territorio_vulneravel),
                    count(distinct {{ coluna_documento }}) filter (
                        where flag_territorio_vulneravel
                    )
                from fct
                group by {{ coluna_ano }}
            {%- endif %}
        )
    select
        '{{ programa }}' as programa,
        c.{{ coluna_ano }},
        c.grupo,
        coalesce(c.valor_grupo, 0) as valor_grupo,
        t.valor_total_ano,
        t.valor_total_com_perfil_ano,
        round(
            coalesce(c.valor_grupo, 0) / nullif(t.valor_total_ano, 0) * 100, 2
        ) as pct_sobre_total,
        round(
            coalesce(c.valor_grupo, 0) / nullif(t.valor_total_com_perfil_ano, 0) * 100, 2
        ) as pct_sobre_com_perfil,
        c.qtd_agentes_grupo,
        round(c.meta_minima * 100, 0) as meta_minima_pct,
        case
            when
                coalesce(c.valor_grupo, 0) / nullif(t.valor_total_com_perfil_ano, 0)
                >= c.meta_minima
            then 'alcancada'
            else 'descumprida'
        end as status_sobre_com_perfil
    from cotas c
    join totais t using ({{ coluna_ano }})
    order by c.{{ coluna_ano }}, c.grupo
{% endmacro %}
