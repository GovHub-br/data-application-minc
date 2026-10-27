{{ config(enabled=false) }}
-- Intermediate — cada pagamento do BB Ágil (silver_fato_bbagil) com o ano de
-- vigência e o município do plano de ação do ente que pagou
-- (silver_plano_acao_ente_ano, do transferegov). Só junta as duas fontes;
-- quem decide se o pagamento casa com perfil e o que conta em cada cota é a gold
-- eixo2_meta3_pagamento_bbagil. LEFT JOIN: pagamento sem plano datado fica com
-- ano_plano NULL, não some. Grão: o de silver_fato_bbagil (1 linha por ente ×
-- documento), porque o plano de ação é único por ente_bbagil.
-- DESABILITADO junto com bronze_fato_bbagil (ver bbagil/).
with pagamentos as (
    select * from {{ ref('silver_fato_bbagil') }}
),
ente_ano as (
    select * from {{ ref('silver_plano_acao_ente_ano') }}
)
select
    p.ente_bbagil,
    p.identificador_unico,
    p.documento_raw,
    p.chave_anonimizada,
    p.valor_pago_num,
    ea.ano_plano,
    ea.codigo_ibge,
    ea.municipio,
    ea.uf
from pagamentos p
left join ente_ano ea
    on p.ente_bbagil = ea.ente_bbagil
