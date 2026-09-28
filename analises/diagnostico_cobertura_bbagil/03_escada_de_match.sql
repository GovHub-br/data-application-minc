-- =============================================================================
-- Q3 — A ESCADA DE MATCH: partição do gap em causas mutuamente exclusivas ⭐
-- =============================================================================
-- Esta é a query central da investigação.
--
-- REGRA DE OURO: um único `motivo_ausencia` por contemplado, atribuído por um
-- CASE em cascata — NUNCA N flags booleanas independentes. Com flags
-- independentes as causas se sobrepõem e a frase "a causa X explica N casos"
-- fica ininterpretável. Com um CASE, os motivos formam uma PARTIÇÃO e somam
-- exatamente o gap.
--
-- ORDEM DA CASCATA (do mais específico para o mais genérico):
--   0_casou                    -> está no indicador
--   1_comprimento_invalido     -> doc não tem 11 nem 14 dígitos (Q2c)
--   2_cortado_pelo_piso_375    -> pago no MESMO programa, mas SUM(valor) < 375
--   3_pago_em_outro_programa   -> passou o piso, mas em programa diferente
--   4_outro_programa_sob_piso  -> pago em outro programa E sob o piso
--   5_cortado_na_limpeza       -> está no extrato bruto, morreu numa das 8 regras
--   6_ausente_do_extrato       -> de fato não há pagamento nenhum
--
-- ATENÇÃO ao motivo 3: programa_fomento sai de LIKE 'MINC-LPG%' /
-- 'MINC-PNAB%' / '%A BLANC%' (pagamentos_bbagil_beneficiarios.sql:30-35).
-- Contemplado LPG pago por plano classificado como PNAB é um não-match
-- IDÊNTICO a "nunca recebeu" — e é corrigível com uma linha de SQL.
--
-- SCHEMA: `bsc` confirmado no banco do MinC (ver 00_cobertura_extracao.sql).
-- =============================================================================

with contemplados as (
    select id_normalizado, programa_fomento
    from agentes.identificadores_contemplados
),

-- passou o piso, mesmo programa -> é exatamente o numerador publicado
pago_mesmo_prog as (
    select distinct beneficiario_documento, programa_fomento
    from agentes.primeiro_pagamento_bancario
),

-- passou o piso, qualquer programa
pago_qualquer_prog as (
    select distinct beneficiario_documento
    from agentes.primeiro_pagamento_bancario
),

-- pré-piso (pós-limpeza), mesmo programa
pgto_mesmo_prog as (
    select distinct beneficiario_documento, programa_fomento
    from agentes.pagamentos_bbagil_beneficiarios
),

-- pré-piso (pós-limpeza), qualquer programa
pgto_qualquer_prog as (
    select distinct beneficiario_documento
    from agentes.pagamentos_bbagil_beneficiarios
),

-- extrato BRUTO, aplicando só a normalização de documento de
-- bbagil_extrato_filtrado.sql:30-35 (LPAD por beneficiarypersontype)
extrato_bruto as (
    select distinct
        case
            when beneficiarypersontype = '1'
                then lpad(regexp_replace(beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
            when beneficiarypersontype = '2'
                then lpad(regexp_replace(beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
        end as doc
    from bsc.raw_bbagil_extrato_transacoes
    where beneficiarydocumentid is not null
),

classificado as (
    select
        c.programa_fomento,
        c.id_normalizado,
        case
            when pm.beneficiario_documento is not null then '0_casou'
            when length(c.id_normalizado) not in (11, 14) then '1_comprimento_invalido'
            when gm.beneficiario_documento is not null then '2_cortado_pelo_piso_375'
            when pq.beneficiario_documento is not null then '3_pago_em_outro_programa'
            when gq.beneficiario_documento is not null then '4_outro_programa_sob_piso'
            when eb.doc is not null                    then '5_cortado_na_limpeza'
            else                                            '6_ausente_do_extrato'
        end as motivo_ausencia
    from contemplados c
    left join pago_mesmo_prog pm
        on  pm.beneficiario_documento = c.id_normalizado
        and pm.programa_fomento       = c.programa_fomento
    left join pgto_mesmo_prog gm
        on  gm.beneficiario_documento = c.id_normalizado
        and gm.programa_fomento       = c.programa_fomento
    left join pago_qualquer_prog pq on pq.beneficiario_documento = c.id_normalizado
    left join pgto_qualquer_prog gq on gq.beneficiario_documento = c.id_normalizado
    left join extrato_bruto      eb on eb.doc                    = c.id_normalizado
)

-- -----------------------------------------------------------------------------
-- 3a — A PARTIÇÃO DO GAP
-- -----------------------------------------------------------------------------
select
    programa_fomento,
    motivo_ausencia,
    count(*) as contemplados,
    round(100.0 * count(*) / sum(count(*)) over (partition by programa_fomento), 2) as pct_do_denominador
from classificado
group by 1, 2
order by 1, 2;


-- =============================================================================
-- VERIFICAÇÃO — O INVARIANTE MAIS IMPORTANTE DE TODA A INVESTIGAÇÃO
-- A soma dos motivos 1..6 tem que dar EXATAMENTE o gap:
--     LPG:  82.307 − 38.084 = 44.223
--     PNAB: 12.085 − 10.736 =  1.349
-- Se não fechar, o CASE não é uma partição e algum motivo está se sobrepondo —
-- pare e conserte antes de interpretar qualquer percentual.
-- =============================================================================
-- select programa_fomento,
--        count(*)                                                as denominador,
--        count(*) filter (where motivo_ausencia = '0_casou')      as numerador,
--        count(*) filter (where motivo_ausencia <> '0_casou')     as gap
-- from classificado group by 1 order by 1;


-- =============================================================================
-- 3b — REFINAMENTO DO MOTIVO 5: qual das 8 regras de limpeza matou?
-- Roda só sobre quem está no extrato bruto mas não sobreviveu.
-- Regras em bronze/bbagil_extrato_filtrado.sql.
-- =============================================================================
-- with doc_normalizado as (
--     select
--         case
--             when beneficiarypersontype = '1'
--                 then lpad(regexp_replace(beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
--             when beneficiarypersontype = '2'
--                 then lpad(regexp_replace(beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
--         end                                                          as doc,
--         regexp_replace(beneficiarydocumentid, '[^0-9]', '', 'g')     as doc_bruto,
--         beneficiaryname, creditdebitindicator, descriptionname,
--         subtransactionquantity, beneficiarypersontype
--     from bsc.raw_bbagil_extrato_transacoes
-- )
-- select
--     case
--         when beneficiarypersontype not in ('1','2')        then 'r8_persontype_invalido'
--         when subtransactionquantity <> '0'                 then 'r1_tem_subtransacao'
--         when doc_bruto in ('0', '191')                     then 'r2_doc_invalido_ou_bb'
--         when doc_bruto = '3793086100189'                   then 'r4_devolucao_fnc'
--         when creditdebitindicator <> 'D'                   then 'r6_nao_e_debito'
--         when descriptionname in (
--                 'BB-APLIC C.PRZ-APL.AUT', 'Resgate Automatico', 'Impostos',
--                 'ORDEM BANC CANCELADA', 'Ordem Bancaria', 'Resgate BB Fix',
--                 'CREDITO CONVENIO', 'Estorno Resgate Automatico')
--                                                            then 'r3_transacao_interna'
--         when upper(coalesce(beneficiaryname, '')) like 'MUNICIPIO%'
--           or upper(coalesce(beneficiaryname, '')) like 'ESTADO%'
--           or upper(coalesce(beneficiaryname, '')) like 'FUNDO%'
--           or upper(coalesce(beneficiaryname, '')) like 'SECRETARIA%'
--           or upper(coalesce(beneficiaryname, '')) like 'SEFAZ%'
--                                                            then 'r7_nome_ente_publico'
--         else 'r5_estorno_ou_sobreviveu'
--     end as regra_que_matou,
--     count(distinct doc) as docs
-- from doc_normalizado
-- where doc in (select id_normalizado from agentes.identificadores_contemplados)
--   and doc not in (select beneficiario_documento from agentes.pagamentos_bbagil_beneficiarios)
-- group by 1 order by 2 desc;


-- =============================================================================
-- 3c — REGRA 7: falsos positivos? Um coletivo cultural chamado "Fundo ..."
-- seria cortado pela mesma regra que exclui repasse entre entes públicos.
-- Se algum destes nomes for contemplado real, a regra precisa de exceção.
-- =============================================================================
-- select beneficiaryname, count(*) as transacoes, sum(value::numeric) as valor
-- from bsc.raw_bbagil_extrato_transacoes
-- where (upper(coalesce(beneficiaryname,'')) like 'FUNDO%'
--     or upper(coalesce(beneficiaryname,'')) like 'SECRETARIA%')
--   and case
--         when beneficiarypersontype = '1'
--             then lpad(regexp_replace(beneficiarydocumentid,'[^0-9]','','g'), 11, '0')
--         when beneficiarypersontype = '2'
--             then lpad(regexp_replace(beneficiarydocumentid,'[^0-9]','','g'), 14, '0')
--       end in (select id_normalizado from agentes.identificadores_contemplados)
-- group by 1 order by 3 desc nulls last limit 50;
--
-- Qualquer linha aqui é um contemplado da lista oficial sendo descartado por
-- causa do nome — falso positivo confirmado, não hipótese.
