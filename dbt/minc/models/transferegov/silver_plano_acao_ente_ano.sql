{{ config(enabled=false) }}
-- Silver transferegov — ano e município do plano de ação por ENTE (id_plano_acao),
-- para datar e localizar o extrato do BB Ágil, que não traz nem ano nem município.
-- DESABILITADO junto com silver_fato_bbagil (bbagil/) até a extração do fato do
-- BB Ágil rodar. enabled=true depois, aqui e em int_bbagil_transferegov.
--
-- ano_plano = ano do INÍCIO DA VIGÊNCIA do plano de ação. É aproximação (mede o
-- ciclo do plano, não a data exata do edital), por isso origem_ano='vigencia_plano'
-- marca menor precisão. codigo_ibge destrava o cruzamento com município.
with planos as (
    select
        id_plano_acao,
        codigo_ibge_municipio_ente_recebedor_plano_acao as codigo_ibge,
        nome_municipio_ente_recebedor_plano_acao        as municipio,
        uf_ente_recebedor_plano_acao                    as uf,
        data_inicio_vigencia_plano_acao                 as dt_inicio_vigencia
    from {{ ref('bronze_plano_acao_minc') }}
),
com_ano as (
    select
        id_plano_acao,
        codigo_ibge,
        municipio,
        uf,
        -- extrai o ano (AAAA) da vigência, validando faixa [2013,2026].
        -- TEXT (como ano_final do fct_pagamentos_elegiveis) p/ os dois fatos
        -- serem compatíveis e o comparativo/uniões não quebrarem por tipo.
        case
            when substring(dt_inicio_vigencia from '(20[12][0-9])') ~ '^20[12][0-9]$'
                 and substring(dt_inicio_vigencia from '(20[12][0-9])')::int between 2013 and 2026
                then substring(dt_inicio_vigencia from '(20[12][0-9])')
        end as ano_plano
    from planos
)
select
    -- ente_bbagil no fato é TEXT (id_plano_acao); casta p/ casar no join
    id_plano_acao::text as ente_bbagil,
    codigo_ibge,
    municipio,
    uf,
    ano_plano
from com_ano
