{{ config(enabled=false) }}
-- Gold — Meta 3: fato de pagamentos VIA BANCO (valor PAGO ao beneficiário final)
-- cruzado com perfil raça/PCD. Espelha eixo2_meta3_pagamento_elegivel
-- (payment-first, LEFT JOIN, preserva órfãos sem perfil) mas usa o BB Ágil como
-- lado-valor, o denominador CORRETO p/ cotas (pago a pessoas, não repasse a entes).
-- A junção BB Ágil × plano de ação já vem de int_bbagil_transferegov; aqui só a
-- regra: casou com perfil, e as flags de cota.
-- DESABILITADO junto com bronze_fato_bbagil (ver bbagil/). enabled=true depois.
--
-- Roda em PARALELO a eixo2_meta3_pagamento_elegivel (não o substitui ainda).
-- eixo2_meta3_comparativo_recebido_vs_pago confronta os dois. A decisão de trocar
-- o denominador oficial das cotas p/ este é do lab (ver plano_integracao_meta3.md).
with
    pagamentos as (select * from {{ ref("int_bbagil_transferegov") }}),
    perfil as (select * from {{ ref("eixo2_meta3_perfil_agente") }})
select
    p.identificador_unico,
    p.ente_bbagil,
    p.valor_pago_num,
    p.chave_anonimizada,
    p.ano_plano as ano_final,
    'vigencia_plano' as origem_ano,
    p.codigo_ibge,
    p.municipio,
    p.uf,
    'pnab_bbagil' as origem,
    (pf.identificador_unico is not null) as tem_perfil,
    coalesce(pf.flag_negra, false) as flag_negra,
    coalesce(pf.flag_indigena, false) as flag_indigena,
    coalesce(pf.is_pcd, false) as flag_pcd
from pagamentos p
left join
    perfil pf
    on p.identificador_unico = pf.identificador_unico
    and p.identificador_unico is not null
