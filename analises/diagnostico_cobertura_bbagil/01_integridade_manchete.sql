-- =============================================================================
-- Q1 — Integridade da manchete: o "99,63% de novos entrantes" sobrevive?
-- =============================================================================
-- CONTEXTO: agentes/gold/primeiro_pagamento_bancario.sql:39-44 calcula
--
--     MIN(data_primeiro_pagamento) OVER (PARTITION BY beneficiario_documento)
--
-- sobre um universo que contém EXATAMENTE DOIS valores de programa_fomento
-- (silver/pagamentos_bbagil_beneficiarios.sql:30-35 anula todo o resto).
-- Logo `categoria = 'Não'` significa literalmente "recebeu o OUTRO programa
-- antes", e quem aparece em um só programa é SEMPRE 'Sim'.
--
-- Os três checks abaixo medem o tamanho do estrago. Se qualquer um deles mover
-- 99,63% / 76,43% de forma material, a prioridade muda de "explicar os 54%
-- faltantes" para "corrigir o número que já publicamos".
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1a — A TAUTOLOGIA
-- Se n_programas = 1 for 100% 'Sim', está provado: "novo entrante" não mede
-- novidade do agente, mede em quantos dos dois programas ele aparece.
-- -----------------------------------------------------------------------------
select
    n_programas,
    categoria_primeiro_acesso_bancario,
    count(*)                                                  as docs,
    round(100.0 * count(*) / sum(count(*)) over (), 2)        as pct_total
from (
    select
        beneficiario_documento,
        categoria_primeiro_acesso_bancario,
        count(*) over (partition by beneficiario_documento) as n_programas
    from agentes.primeiro_pagamento_bancario
) t
group by 1, 2
order by 1, 2;

-- Leitura esperada:
--   n_programas = 1  ->  100% 'Sim'   (não há como ser 'Não': não há com o que comparar)
--   n_programas = 2  ->  1 'Sim' + 1 'Não' por documento, decidido só pela data


-- -----------------------------------------------------------------------------
-- 1b — EMPATES: quem recebeu os dois no mesmo dia conta como estreante 2x
-- data_pagamento é DATE (TO_DATE(bookingdate,'DD/MM/YYYY')), sem hora.
-- Empate satisfaz `data = MIN(data)` nas DUAS linhas -> 'Sim' em ambas,
-- inflando o numerador dos dois programas simultaneamente.
-- -----------------------------------------------------------------------------
select
    count(*)                                    as docs_com_empate,
    (select count(distinct beneficiario_documento)
     from agentes.primeiro_pagamento_bancario)  as docs_total
from (
    select beneficiario_documento
    from agentes.primeiro_pagamento_bancario
    group by 1
    having count(*) = 2
       and min(data_primeiro_pagamento) = max(data_primeiro_pagamento)
) t;


-- -----------------------------------------------------------------------------
-- 1c — O PISO DE R$375 FABRICA ESTREANTES
-- Em primeiro_pagamento_bancario.sql:28-32 o CTE `acima_limiar` filtra ANTES
-- da window function. Alguém com LPG R$200 (linha inteira descartada) e PNAB
-- R$5.000 perde a linha LPG e a linha PNAB VIRA 'Sim'.
-- O piso não só apara ruído: ele cria novos entrantes.
--
-- Recalcula a categoria com piso 0 e com piso 375 e compara.
-- -----------------------------------------------------------------------------
with agregado as (
    select
        beneficiario_documento,
        programa_fomento,
        min(data_pagamento) as data_primeiro_pagamento,
        sum(valor)          as valor_total_pago
    from agentes.pagamentos_bbagil_beneficiarios
    group by 1, 2
),

com_piso_0 as (
    select
        beneficiario_documento,
        programa_fomento,
        case when data_primeiro_pagamento
                  = min(data_primeiro_pagamento) over (partition by beneficiario_documento)
             then 'Sim' else 'Não' end as cat_piso_0
    from agregado
),

com_piso_375 as (
    select
        beneficiario_documento,
        programa_fomento,
        case when data_primeiro_pagamento
                  = min(data_primeiro_pagamento) over (partition by beneficiario_documento)
             then 'Sim' else 'Não' end as cat_piso_375
    from agregado
    where valor_total_pago >= 375
)

select
    coalesce(p375.programa_fomento, p0.programa_fomento) as programa_fomento,
    p0.cat_piso_0,
    p375.cat_piso_375,
    count(*) as docs
from com_piso_0 p0
full outer join com_piso_375 p375
    on  p375.beneficiario_documento = p0.beneficiario_documento
    and p375.programa_fomento       = p0.programa_fomento
group by 1, 2, 3
order by 1, 2, 3;

-- Olhar especialmente: cat_piso_0 = 'Não' e cat_piso_375 = 'Sim'
-- -> estreantes fabricados pelo piso.


-- =============================================================================
-- VERIFICAÇÃO — 1a tem que reproduzir os números publicados:
--   LPG  37.943 'Sim' / 141 'Não'
--   PNAB  8.206 'Sim' / 2.530 'Não'
-- =============================================================================
-- select programa_fomento, categoria_primeiro_acesso_bancario,
--        count(distinct beneficiario_documento) as docs
-- from agentes.primeiro_pagamento_bancario
-- group by 1, 2 order by 1, 2;
--
-- NOTA: este número é sobre TODOS os beneficiários bancários; os 37.943/141
-- publicados são só os que também estão em identificadores_contemplados
-- (INNER JOIN em primeiro_acesso_contemplados_bancario.sql:25-34).
-- Para bater exato, junte com agentes.identificadores_contemplados.
