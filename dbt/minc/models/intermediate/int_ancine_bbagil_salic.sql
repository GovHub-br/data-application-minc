{{ config(materialized="table") }}

-- Intermediate — junta numa forma só os eventos em que um agente recebeu
-- recurso, vindos de três fontes:
-- BB_AGIL  saída de dinheiro das contas dos planos de ação (silver_pagamento_bbagil)
-- SALIC    recibo de captação da Lei Rouanet (silver_sac_captacao_proponente)
-- ANCINE   captação de projeto audiovisual (silver_consulta)
--
-- Grão: 1 linha por evento da fonte. Nada é descartado nem classificado aqui
-- (ADR 0010): quais eventos contam, a que política cada um pertence e o piso
-- de valor são regra de negócio e ficam na Gold. Por isso cada linha leva,
-- além do comum (documento, data, valor), o que a fonte tem para essa
-- decisão — programa_bruto, beneficiario_nome, beneficiario_documento_bruto,
-- descriptionname, tipo_lancamento, creditdebitindicator,
-- status_prestacao_contas e a conta (id_plano_acao_dado_bancario) —, nulo
-- onde a fonte não tem.
--
-- As linhas do BB Ágil chegam mesmo sem documento normalizado ou sem data:
-- elas ainda podem ser o outro lado de um estorno, e a Gold só as descarta
-- depois de parear (o pareamento é por conta, e precisa das duas pernas).
-- SALIC e Ancine já chegam com documento e data das suas silvers.
with
    bbagil as (
        select
            'BB_AGIL' as fonte,
            programa_curto as programa_bruto,
            beneficiario_documento,
            beneficiario_documento_bruto,
            beneficiario_nome,
            data_pagamento as data_evento,
            valor,
            tipo_lancamento,
            descriptionname,
            creditdebitindicator,
            status_prestacao_contas,
            id_plano_acao,
            id_plano_acao_dado_bancario,
            null::text as pronac,
            null::text as no_salic
        from {{ ref("silver_pagamento_bbagil") }}
    ),

    salic as (
        select
            'SALIC' as fonte,
            null::text as programa_bruto,
            proponente_documento as beneficiario_documento,
            null::text as beneficiario_documento_bruto,
            proponente_nome as beneficiario_nome,
            data_recibo as data_evento,
            valor_captado as valor,
            null::text as tipo_lancamento,
            null::text as descriptionname,
            null::text as creditdebitindicator,
            null::text as status_prestacao_contas,
            null::integer as id_plano_acao,
            null::integer as id_plano_acao_dado_bancario,
            pronac,
            null::text as no_salic
        from {{ ref("silver_sac_captacao_proponente") }}
    ),

    ancine as (
        select
            'ANCINE' as fonte,
            mecanismo as programa_bruto,
            proponente_documento as beneficiario_documento,
            null::text as beneficiario_documento_bruto,
            proponente_nome as beneficiario_nome,
            data_captacao as data_evento,
            valor_captacao as valor,
            null::text as tipo_lancamento,
            null::text as descriptionname,
            null::text as creditdebitindicator,
            null::text as status_prestacao_contas,
            null::integer as id_plano_acao,
            null::integer as id_plano_acao_dado_bancario,
            null::text as pronac,
            no_salic
        from {{ ref("silver_consulta") }}
    ),

    unificado as (
        select *
        from bbagil
        union all
        select *
        from salic
        union all
        select *
        from ancine
    )

select
    fonte,
    programa_bruto,
    beneficiario_documento,
    beneficiario_documento_bruto,
    beneficiario_nome,
    case
        when length(beneficiario_documento) = 11
        then 'PF'
        when beneficiario_documento is not null
        then 'PJ'
    end as tipo_pessoa,
    data_evento,
    extract(year from data_evento)::int as ano_evento,
    valor,
    tipo_lancamento,
    descriptionname,
    creditdebitindicator,
    status_prestacao_contas,
    id_plano_acao,
    id_plano_acao_dado_bancario,
    pronac,
    no_salic
from unificado
