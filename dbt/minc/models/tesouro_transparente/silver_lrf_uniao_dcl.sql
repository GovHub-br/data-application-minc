-- Silver tesouro_transparente — lrf_uniao_dcl.
-- Origem: bronze_lrf_uniao_dcl, cópia fiel de
-- seeds.tesouro_transparente_lrf_uniao_dcl, onde tudo chega como text da carga
-- de fonte estática (tesouro_transparente_dag, ADR 0012). Tipar é o trabalho
-- desta camada.
--
-- O período vira ano e quadrimestre. O texto é sempre 'AAAA - Até o Nº
-- Quadrimestre'; o que não casa fica nulo, e os testes not_null acusam.
--
-- Os valores ficam na unidade do Tesouro, milhares de reais, e o nome da
-- coluna diz isso (_mil). O CSV usa vírgula decimal e não tem separador de
-- milhar ('558706386,6'), por isso o cast é feito aqui e não pela
-- bronze_numerico (só ponto) nem pela parse_valor (que lê ',d' e ',dd' como
-- decimal, mas tira a vírgula de '5,717955581', e devolve nulo acima de
-- 10.000.000).
with

    origem as (
        select
            *, trim(periodo) ~ '^[0-9]{4} - Até o [1-3]º Quadrimestre$' as periodo_valido
        from {{ ref("bronze_lrf_uniao_dcl") }}
    )

select
    {{ bronze_texto("periodo") }} as periodo,
    case when periodo_valido then left(trim(periodo), 4)::integer end as ano,
    case
        when periodo_valido then substring(trim(periodo) from 'Até o ([1-3])º')::integer
    end as quadrimestre,
    case
        when trim(divida_consolidada_dc_i) ~ '^-?[0-9]+(,[0-9]+)?$'
        then replace(trim(divida_consolidada_dc_i), ',', '.')::numeric
    end as divida_consolidada_mil,
    case
        when trim(deducoes_ii) ~ '^-?[0-9]+(,[0-9]+)?$'
        then replace(trim(deducoes_ii), ',', '.')::numeric
    end as deducoes_mil,
    case
        when trim(divida_consolidada_liquida_dcl_iii_i_ii) ~ '^-?[0-9]+(,[0-9]+)?$'
        then replace(trim(divida_consolidada_liquida_dcl_iii_i_ii), ',', '.')::numeric
    end as divida_consolidada_liquida_mil,
    case
        when trim(receita_corrente_liquida_rcl) ~ '^-?[0-9]+(,[0-9]+)?$'
        then replace(trim(receita_corrente_liquida_rcl), ',', '.')::numeric
    end as receita_corrente_liquida_mil,
    case
        when trim(da_dc_sobre_a_rcl_i_rcl) ~ '^-?[0-9]+(,[0-9]+)?$'
        then replace(trim(da_dc_sobre_a_rcl_i_rcl), ',', '.')::numeric
    end as razao_dc_rcl,
    case
        when trim(da_dcl_sobre_a_rcl_iii_rcl) ~ '^-?[0-9]+(,[0-9]+)?$'
        then replace(trim(da_dcl_sobre_a_rcl_iii_rcl), ',', '.')::numeric
    end as razao_dcl_rcl,
    {{ bronze_data("versao") }} as versao,
    {{ bronze_texto("recurso_id") }} as recurso_id,
    {{ bronze_texto("dt_ingest") }} as dt_ingest
from origem
