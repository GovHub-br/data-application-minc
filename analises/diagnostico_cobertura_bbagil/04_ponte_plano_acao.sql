-- =============================================================================
-- Q4 — A PONTE contemplado → ente: listado × pago no mesmo plano de ação
-- =============================================================================
-- A ponte existe no banco e NÃO está implementada em nenhum modelo dbt:
--
--   lpg_contemplados.id_anexo  (texto 'anexo_<N>')
--     -> substring(id_anexo from 'anexo_([0-9]+)') = anexos_relatorios.id
--     -> relatorios_gestao.id_plano_acao
--     -> raw_planos_acao.{uf, município, código IBGE, ente, vigência}
--
-- É estritamente N:1 (as três tabelas têm upsert por PK, confirmado nas DAGs),
-- e id_plano_acao JÁ EXISTE do lado bancário (bbagil_extrato_filtrado.sql:59-66).
-- Dá para comparar listado × pago DENTRO DO MESMO plano de ação.
--
-- CUIDADOS:
-- 1. id_plano_acao é TEXT e anexos_relatorios.id é numérico. Compare como TEXT
--    ou proteja o cast — cotas_dbt/silver/bbagil_ente_ano.sql:18 já resolveu
--    esse mesmo problema com ::text.
-- 2. FAN-OUT do lado do contemplado: um documento pode estar em vários anexos
--    (resultado preliminar + final) -> várias linhas. Use count(distinct ...),
--    NUNCA count(*). sum(valor) superconta se o prêmio estiver em dois anexos.
-- 3. DESCONTE A Q0: taxa de match por plano só é interpretável para planos com
--    situacao_extracao = '5_extraido'. Sem isso, "ente com 0% de match" pode
--    ser um ente que nunca foi consultado — acusação falsa contra prefeitura real.
-- 4. O corte temporal certo NÃO é "vigência vs 2023-02". É "vigência vs último
--    mês extraído com sucesso para aquele ente". "Ainda não pago quando
--    extraímos" é achado diferente de "pago fora do BB".
-- 5. COLUNA "cpf ou cnpj" ESTÁ HARDCODED abaixo. Rode a Q2a primeiro: pode
--    existir "cpf ou cnpj " (espaço final), variante com NBSP, e uma coluna
--    "cnpj" separada. Troque por um COALESCE das colunas que a Q2a listar,
--    senão você subestima `contemplados_listados` e a taxa de match sai errada.
-- =============================================================================

with anexo_para_plano as (
    -- protege o cast: só id_anexo no formato esperado
    select
        ar.id::text                     as anexo_id,
        rg.id_plano_acao::text          as id_plano_acao
    from transferegov_fundo_a_fundo.anexos_relatorios ar
    join transferegov_fundo_a_fundo.relatorios_gestao rg
        on rg.id_relatorio_gestao = ar.id_relatorio_gestao
),

plano as (
    select
        pa.id_plano_acao::text                as id_plano_acao,
        pa.id_programa::text                  as id_programa,
        case
            when pa.id_programa::text in ('46','47')       then 'LPG'
            when pa.id_programa::text in ('60','61','62')  then 'PNAB'
        end                                   as programa,
        pa.nome_ente_recebedor_plano_acao     as ente,
        pa.uf_ente_recebedor_plano_acao       as uf,
        pa.codigo_ibge_municipio_ente_recebedor_plano_acao as codigo_ibge,
        pa.data_inicio_vigencia_plano_acao    as inicio_vigencia
    from transferegov_fundo_a_fundo.raw_planos_acao pa
    where pa.id_programa::text in ('46','47','60','61','62')
),

-- situação de extração vinda da Q0 — sem isto a leitura da taxa de match é inválida
extracao as (
    select
        cx.id_plano_acao::text                       as id_plano_acao,
        count(*) filter (where cx.status = 'ok')     as meses_ok,
        max(cx.periodo_final)                        as ultimo_periodo_extraido
    from bsc.controle_extracao_bbagil_extrato cx
    group by 1
),

-- contemplados LPG com o plano de ação resolvido pela ponte
contemplados_com_plano as (
    select distinct
        ap.id_plano_acao,
        regexp_replace(coalesce(lc."cpf ou cnpj", ''), '[^0-9]', '', 'g') as id_normalizado
    from transferegov_fundo_a_fundo.lpg_contemplados lc
    join anexo_para_plano ap
        on ap.anexo_id = substring(lc.id_anexo from 'anexo_([0-9]+)')
    where lc.id_anexo ~ '^anexo_[0-9]+'
      and length(regexp_replace(coalesce(lc."cpf ou cnpj", ''), '[^0-9]', '', 'g')) >= 11
),

-- pagamentos do BB Ágil, com plano de ação preservado (a agregação de
-- primeiro_pagamento_bancario descarta id_plano_acao — por isso lemos o silver)
pagos_com_plano as (
    select distinct
        id_plano_acao::text    as id_plano_acao,
        beneficiario_documento as id_normalizado
    from agentes.pagamentos_bbagil_beneficiarios
),

por_plano as (
    select
        p.programa,
        p.uf,
        p.ente,
        p.id_plano_acao,
        p.inicio_vigencia,
        coalesce(e.meses_ok, 0)         as meses_extraidos_ok,
        e.ultimo_periodo_extraido,
        count(distinct c.id_normalizado) as contemplados_listados,
        count(distinct c.id_normalizado) filter (
            where pg.id_normalizado is not null
        )                                as contemplados_pagos
    from plano p
    left join extracao e                on e.id_plano_acao = p.id_plano_acao
    left join contemplados_com_plano c  on c.id_plano_acao = p.id_plano_acao
    left join pagos_com_plano pg
        on  pg.id_plano_acao  = c.id_plano_acao
        and pg.id_normalizado = c.id_normalizado
    group by 1, 2, 3, 4, 5, 6, 7
)

-- -----------------------------------------------------------------------------
-- 4a — Taxa de match por plano, SÓ entre os planos efetivamente extraídos
-- -----------------------------------------------------------------------------
select
    programa,
    uf,
    count(*)                              as planos,
    sum(contemplados_listados)            as listados,
    sum(contemplados_pagos)               as pagos,
    round(100.0 * sum(contemplados_pagos) / nullif(sum(contemplados_listados), 0), 2) as pct_match
from por_plano
where meses_extraidos_ok > 0          -- <<< o desconto da Q0
  and contemplados_listados > 0
group by 1, 2
order by 1, pct_match asc;


-- -----------------------------------------------------------------------------
-- 4b — Os candidatos REAIS a "pagou fora do canal BB":
-- plano extraído com sucesso, tem contemplados listados, e ZERO deles recebeu.
-- -----------------------------------------------------------------------------
-- select programa, uf, ente, id_plano_acao, inicio_vigencia,
--        meses_extraidos_ok, ultimo_periodo_extraido, contemplados_listados
-- from por_plano
-- where meses_extraidos_ok > 0
--   and contemplados_listados >= 5      -- ignora ruído de planos minúsculos
--   and contemplados_pagos = 0
-- order by contemplados_listados desc
-- limit 100;


-- -----------------------------------------------------------------------------
-- 4c — O corte temporal correto: vigência do plano vs último mês extraído
-- "Ainda não pago quando olhamos" ≠ "pago fora do BB"
-- -----------------------------------------------------------------------------
-- CUIDADO: inicio_vigencia e ultimo_periodo_extraido são TEXT. O ::date só
-- funciona se o formato for ISO. Confira os dois com um `limit 5` antes e, se
-- vierem em DD/MM/YYYY, troque por to_date(coluna, 'DD/MM/YYYY').
-- select programa,
--        case
--            when ultimo_periodo_extraido is null then 'nunca_extraido'
--            when inicio_vigencia::date > ultimo_periodo_extraido::date
--                 then 'vigencia_posterior_a_extracao'
--            else 'janela_coberta'
--        end as situacao_temporal,
--        count(*) as planos,
--        sum(contemplados_listados) as listados,
--        sum(contemplados_pagos)    as pagos
-- from por_plano
-- group by 1, 2 order by 1, 2;


-- =============================================================================
-- VERIFICAÇÃO — o fan-out não pode criar pessoas:
-- count(distinct id_normalizado) agregado sobre todos os planos tem que ser
-- <= o denominador da Q3.
-- =============================================================================
-- select count(distinct id_normalizado) as docs_via_ponte
-- from contemplados_com_plano;
--
-- E quantos contemplados a ponte NÃO consegue resolver (anexo órfão):
-- select count(*) filter (where ap.anexo_id is null) as sem_plano_resolvido,
--        count(*)                                    as total
-- from transferegov_fundo_a_fundo.lpg_contemplados lc
-- left join anexo_para_plano ap
--     on ap.anexo_id = substring(lc.id_anexo from 'anexo_([0-9]+)');
