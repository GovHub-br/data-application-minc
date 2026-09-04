-- =============================================================================
-- Q6 — PERFIL DO GAP: perguntas substantivas
-- =============================================================================
-- SÓ RODE DEPOIS de Q0–Q3. Estas queries assumem que a população já foi limpa
-- (planos nunca extraídos descontados, denominador corrigido, motivos
-- particionados). Rodar antes disso produz números que serão jogados fora.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 6a — O GAP É DE PESSOAS OU DE REAIS?
-- 46% das PESSOAS. E dos REAIS? Se os faltantes forem prêmios pequenos, a
-- cobertura de valor pode ser muito maior — e isso muda completamente como o
-- resultado deve ser comunicado.
-- cotas.stg_contemplados_lpg.valor_pago_num já existe (parse_valor com teto R$10M).
-- -----------------------------------------------------------------------------
with contemplado_valor as (
    select
        'LPG'                       as programa_fomento,
        identificador_unico         as id_normalizado,
        sum(valor_pago_num)         as valor_listado
    from cotas.stg_contemplados_lpg
    where identificador_unico is not null
      and valor_pago_num is not null
    group by 1, 2

    union all

    select
        'PNAB'                      as programa_fomento,
        identificador_unico         as id_normalizado,
        sum(valor_pago_num)         as valor_listado
    from cotas.stg_contemplados_pnab
    where identificador_unico is not null
      and valor_pago_num is not null
    group by 1, 2
)
select
    cv.programa_fomento,
    case when pb.beneficiario_documento is not null then 'apareceu' else 'nao_apareceu' end as situacao,
    count(*)                    as contemplados,
    sum(cv.valor_listado)       as valor_listado,
    round(100.0 * sum(cv.valor_listado)
          / sum(sum(cv.valor_listado)) over (partition by cv.programa_fomento), 2) as pct_valor,
    round(avg(cv.valor_listado), 2) as ticket_medio
from contemplado_valor cv
left join agentes.primeiro_pagamento_bancario pb
    on  pb.beneficiario_documento = cv.id_normalizado
    and pb.programa_fomento       = cv.programa_fomento
group by 1, 2
order by 1, 2;

-- Se pct_valor de 'apareceu' for muito maior que 46%, a manchete correta é
-- "cobrimos X% do dinheiro", não "cobrimos 46% dos contemplados".


-- -----------------------------------------------------------------------------
-- 6b — DIMENSÕES QUE EXISTEM NA RAW E NENHUM MODELO dbt LÊ
-- lpg_contemplados.categoria_contemplado  (ffill de sub-tabelas, extracao_planilhas.py:1132)
-- raw_pnab_*.tipo_edital
-- Cruzar contra o motivo_ausencia da Q3 pode revelar que o gap se concentra
-- numa modalidade específica.
-- -----------------------------------------------------------------------------
-- select lc.categoria_contemplado,
--        case when pb.beneficiario_documento is not null then 'apareceu' else 'nao_apareceu' end as situacao,
--        count(distinct regexp_replace(lc."cpf ou cnpj", '[^0-9]', '', 'g')) as contemplados
-- from transferegov_fundo_a_fundo.lpg_contemplados lc
-- left join agentes.primeiro_pagamento_bancario pb
--     on  pb.beneficiario_documento = regexp_replace(lc."cpf ou cnpj", '[^0-9]', '', 'g')
--     and pb.programa_fomento = 'LPG'
-- where length(regexp_replace(coalesce(lc."cpf ou cnpj", ''), '[^0-9]', '', 'g')) >= 11
-- group by 1, 2
-- order by 1, 2;


-- -----------------------------------------------------------------------------
-- 6c — COLETIVOS COM CHAVE TROCADA
-- A ficha do coletivo é indexada pelo CPF do representante
-- (cotas_dbt/bronze/stg_agentes_coletivos.sql), mas o pagamento pode sair no
-- CNPJ do coletivo — ou o inverso. Não é corrigível por normalização.
--
-- TESTE de alta precisão: para um contemplado NÃO casado, o MESMO plano de ação
-- contém pagamento a um documento diferente cujo beneficiario_nome bate com o
-- "nome do(a) contemplado(a)" da lista? Dentro de um plano o conjunto candidato
-- é minúsculo, então o match por nome é confiável. É o melhor uso da ponte da Q4.
-- -----------------------------------------------------------------------------
-- with contemplado_no_plano as (
--     select
--         rg.id_plano_acao::text                                             as id_plano_acao,
--         regexp_replace(lc."cpf ou cnpj", '[^0-9]', '', 'g')                as doc_listado,
--         upper(trim(lc."nome do(a) contemplado(a)"))                        as nome_listado
--     from transferegov_fundo_a_fundo.lpg_contemplados lc
--     join transferegov_fundo_a_fundo.anexos_relatorios ar
--         on ar.id::text = substring(lc.id_anexo from 'anexo_([0-9]+)')
--     join transferegov_fundo_a_fundo.relatorios_gestao rg
--         on rg.id_relatorio_gestao = ar.id_relatorio_gestao
--     where lc.id_anexo ~ '^anexo_[0-9]+'
-- )
-- select c.id_plano_acao, c.doc_listado, c.nome_listado,
--        ef.beneficiario_documento as doc_pago, ef.beneficiario_nome
-- from contemplado_no_plano c
-- join agentes.bbagil_extrato_filtrado ef
--     on  ef.id_plano_acao::text = c.id_plano_acao
--     and upper(trim(ef.beneficiario_nome)) = c.nome_listado
--     and ef.beneficiario_documento <> c.doc_listado
-- where c.doc_listado not in (select beneficiario_documento from agentes.primeiro_pagamento_bancario)
-- limit 100;
--
-- Cada linha aqui é um contemplado que RECEBEU, contado hoje como "não apareceu"
-- só porque a lista e o extrato usam chaves de tipos diferentes.


-- -----------------------------------------------------------------------------
-- 6d — OS 141 "VETERANOS" DA LPG
-- Lembre da Q1: eles não são veteranos. São as 141 pessoas cujo pagamento PNAB
-- antecede o LPG. n pequeno o bastante para olhar caso a caso — trate como
-- LISTA DE ANOMALIAS (data ruim, programa_curto mal classificado, piloto PNAB
-- precoce), não como evidência de reincidência no fomento.
-- -----------------------------------------------------------------------------
select
    pb.beneficiario_documento,
    pb.programa_fomento,
    pb.data_primeiro_pagamento,
    pb.data_primeiro_pagamento_geral,
    pb.valor_total_pago,
    pb.categoria_primeiro_acesso_bancario
from agentes.primeiro_pagamento_bancario pb
where pb.beneficiario_documento in (
    select beneficiario_documento
    from agentes.primeiro_pagamento_bancario
    where programa_fomento = 'LPG'
      and categoria_primeiro_acesso_bancario = 'Não'
)
order by pb.beneficiario_documento, pb.data_primeiro_pagamento;

-- Olhar: as datas PNAB são plausíveis (>= 2023-02) ou há data corrompida?
-- O programa_curto de origem dessas transações realmente é PNAB?
