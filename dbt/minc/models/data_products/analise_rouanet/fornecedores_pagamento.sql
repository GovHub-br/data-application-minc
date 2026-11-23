-- Gold / analise_rouanet -- pagamentos a fornecedores de cada projeto, cruzados
-- com a situacao do CNPJ na base de empresas do SALIC, sem identidade.
--
-- GRAO: 1 linha por PRONAC. UNIVERSO: os PRONACs com ao menos um pagamento em
-- `silver_sac_pagamento_profissional_rouanet` que passe nos filtros dos notebooks
-- do analytics_minc: valor pago maior que zero, primeira ocorrencia da chave
-- candidata (descarta a duplicata) e data de pagamento de 2013 ate hoje. O ano
-- vai ate `current_date` e nao ate um ano fixo, e a data futura fica fora.
--
-- A BASE DE EMPRESAS E A DO SALIC (`silver_agentes_vwempresajuridica`), uma copia
-- unica de 24/08/2026, uma linha por CNPJ. Tres limites dela moldam o modelo:
-- * cobertura: casa com 90,5% do valor pago a PJ de 2013 a 2026, mas so 49,5% a
-- 64,3% de 2013 a 2017. Ela parece encher quando o SALIC consulta a Receita, o
-- que so vale para todo comprovante desde 2023. Por isso cada linha traz
-- `valor_pago_pj_sem_base`, o que nao pode ser classificado;
-- * a situacao e a de 24/08/2026. "Irregular na data" quer dizer: a situacao de
-- hoje nao e ativa e a data dela e anterior ou igual a do pagamento. Uma
-- empresa que ficou inapta e voltou a ativa antes da copia nao aparece;
-- * MEI e Simples tambem sao os de hoje, por isso o sufixo `_atual`. A baixa
-- apaga a marca de MEI (so 16 das 7.170 baixadas aparecem como MEI), entao
-- estas colunas nao dizem se o fornecedor era MEI quando recebeu.
-- A natureza juridica ficou de fora: em 82,7% dos CNPJs o campo guarda o codigo
-- da situacao cadastral, e so 4,2% do valor pago tem natureza valida.
--
-- O DOCUMENTO SO E LIDO, NUNCA SAI. `documento_prestador` e `tipo_pessoa_prestador`
-- (silver restrita de pagamentos) e `nrcnpj` (base de empresas) servem para casar
-- e classificar, e nao entram no select final. O casamento e pelo CNPJ inteiro de
-- 14 digitos, entao matriz e filial nao se misturam.
--
-- DATAS-SENTINELA. 1901-01-01 na data de abertura ou de situacao e "sem data" e
-- vira nulo; com data nula a regra daquela coluna nao se aplica.
with

    pagamento as (
        select
            pronac,
            data_pagamento,
            valor_pago_reais as valor,
            tipo_pessoa_prestador as tipo_pessoa,
            documento_prestador as documento
        from {{ ref("silver_sac_pagamento_profissional_rouanet") }}
        where
            pronac is not null
            and valor_pago_reais > 0
            and ocorrencia_chave_candidata = 1
            and ano_pagamento >= 2013
            and data_pagamento <= current_date
    ),

    empresa as (
        select
            {{ normaliza_documento("nrcnpj") }} as cnpj,
            -- 1 nula, 3 suspensa, 4 inapta, 8 baixada; 2 e ativa
            cdsituacaocadastral in ('1', '3', '4', '8') as irregular,
            cdsituacaocadastral = '8' as baixada,
            case
                when dtsituacaocadastral > '1901-01-01' then dtsituacaocadastral
            end as data_situacao,
            case when dtabertura > '1901-01-01' then dtabertura end as data_abertura,
            sioptantemei = 'SIM' as mei,
            sioptantesimples = 'SIM' as optante_simples
        from {{ ref("silver_agentes_vwempresajuridica") }}
    ),

    classificado as (
        select
            p.pronac,
            p.valor,
            p.tipo_pessoa,
            not {{ documento_valido("p.documento") }} as documento_invalido,
            e.cnpj is not null as na_base,
            e.mei,
            e.optante_simples,
            e.irregular and e.data_situacao <= p.data_pagamento as irregular_na_data,
            e.baixada
            and e.data_situacao
            < p.data_pagamento - interval '5 years' as baixada_ha_mais_de_5_anos,
            p.data_pagamento < e.data_abertura as antes_da_abertura,
            p.data_pagamento >= e.data_abertura
            and p.data_pagamento < e.data_abertura + 180 as aberta_ha_menos_de_180_dias
        from pagamento as p
        left join empresa as e on p.tipo_pessoa = 'PJ' and p.documento = e.cnpj
    )

select
    pronac,
    count(*) as qt_pagamentos,
    sum(valor) as valor_pago,
    count(*) filter (where tipo_pessoa = 'PJ') as qt_pagamentos_pj,
    coalesce(sum(valor) filter (where tipo_pessoa = 'PJ'), 0) as valor_pago_pj,
    coalesce(
        sum(valor) filter (where tipo_pessoa = 'PJ' and not na_base), 0
    ) as valor_pago_pj_sem_base,
    count(*) filter (where mei) as qt_pagamentos_a_mei_atual,
    coalesce(sum(valor) filter (where mei), 0) as valor_pago_a_mei_atual,
    coalesce(
        sum(valor) filter (where optante_simples), 0
    ) as valor_pago_a_optante_simples_atual,
    count(*) filter (
        where irregular_na_data
    ) as qt_pagamentos_a_empresa_irregular_na_data,
    coalesce(
        sum(valor) filter (where irregular_na_data), 0
    ) as valor_pago_a_empresa_irregular_na_data,
    coalesce(
        sum(valor) filter (where baixada_ha_mais_de_5_anos), 0
    ) as valor_pago_a_empresa_baixada_ha_mais_de_5_anos,
    count(*) filter (where antes_da_abertura) as qt_pagamentos_antes_da_abertura,
    coalesce(
        sum(valor) filter (where aberta_ha_menos_de_180_dias), 0
    ) as valor_pago_a_empresa_aberta_ha_menos_de_180_dias,
    count(*) filter (
        where tipo_pessoa = 'PF' and documento_invalido
    ) as qt_pagamentos_documento_invalido_pf,
    count(*) filter (
        where tipo_pessoa = 'PJ' and documento_invalido
    ) as qt_pagamentos_documento_invalido_pj,
    -- documento vazio ou com tamanho diferente de 11 e 14
    count(*) filter (where tipo_pessoa is null) as qt_pagamentos_sem_documento
from classificado
group by pronac
