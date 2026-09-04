-- =============================================================================
-- Q0 — Cobertura da extração: quanto do gap é "nunca olhamos"?
-- =============================================================================
-- PERGUNTA: das CONTAS bancárias em escopo (LPG + PNAB), quantas foram de fato
-- consultadas no BB Ágil? Um contemplado pode não aparecer no extrato apenas
-- porque a conta do município dele nunca foi consultada.
--
-- A UNIDADE É A CONTA, NÃO O PLANO DE AÇÃO. Até 03/09/2026 a DAG consultava
-- uma conta por plano (`DISTINCT ON (id_plano_acao)`) e planos da LPG têm
-- duas — a segunda nunca era consultada e não deixava rastro nenhum na tabela
-- de controle, então o plano aparecia aqui como '5_extraido'. Esta query
-- contava planos, e por isso não conseguia ver o buraco. De quebra, o
-- `left join banco` no grão de plano duplicava todo plano de duas contas: os
-- números que ela deu antes de 03/09/2026 estão inflados para a LPG.
--
-- POR QUE ESTA É A PRIMEIRA: se a Q4 (taxa de match por ente) rodar antes
-- disso, "ente com 0% de match" vira acusação de que a prefeitura pagou fora
-- do canal BB — quando pode ser que a gente simplesmente nunca perguntou.
--
-- MECÂNICA (dags/data_ingest/bsc_pnab/extracao_bbagil_dag.py):
--   _SQL_CONTAS_POR_PLANO -> uma entrada por conta distinta (agência+conta) de
--                            cada plano, com agência E conta não-nulas
--   _combinacoes_extrato_pendentes -> checkpoint por (plano × conta × mês)
--   CHAVE_CONTROLE_EXTRATO -> PK (id_plano_acao, id_plano_acao_dado_bancario,
--                                 periodo_inicial, periodo_final)
--   status ∈ ('ok', 'sem_dados', 'erro') — 'sem_dados' cobre "período sem
--   lançamentos" e "conta corrente inválida", distinguidos por mensagem_erro
--
-- SCHEMA: os nomes abaixo são os que o código escreve hoje
-- (plugins/schemas_minc.py): `transferegov.plano_acao_dado_bancario_minc`,
-- `transferegov.plano_acao_minc` e `bbagil.*`. O dump antigo para o banco do
-- MinC usava `transferegov_fundo_a_fundo.raw_planos_acao*` e `bsc.raw_bbagil_*`
-- (com as colunas curtas `agencia`/`conta`/`situacao_conta`) — se for esse o
-- banco em que você está rodando, troque os nomes nos três CTEs abaixo.
-- =============================================================================

with planos_escopo as (
    -- id_programa é TEXT e é comparado como string na DAG (download_anexos_dag.py:53-58)
    select
        pa.id_plano_acao::text            as id_plano_acao,
        pa.id_programa::text              as id_programa,
        case
            when pa.id_programa::text in ('46', '47')       then 'LPG'
            when pa.id_programa::text in ('60', '61', '62') then 'PNAB'
        end                               as programa,
        pa.uf_ente_recebedor_plano_acao   as uf,
        pa.nome_ente_recebedor_plano_acao as ente,
        pa.data_inicio_vigencia_plano_acao as inicio_vigencia
    from transferegov.plano_acao_minc pa
    where pa.id_programa::text in ('46', '47', '60', '61', '62')
),

-- Uma linha por CONTA distinta de cada plano — mesma deduplicação que a DAG
-- faz em _SQL_CONTAS_POR_PLANO (a mesma conta pode estar cadastrada duas vezes
-- com id_plano_acao_dado_bancario diferentes; a canônica é a ativa, senão a de
-- maior id).
banco as (
    select distinct on (id_plano_acao, agencia, conta) *
    from (
        select
            id_plano_acao::text                                              as id_plano_acao,
            id_plano_acao_dado_bancario::text                                as id_conta,
            nullif(trim(numero_agencia_plano_acao_dado_bancario::text), '')  as agencia,
            nullif(trim(numero_conta_plano_acao_dado_bancario::text), '')    as conta,
            situacao_conta_plano_acao_dado_bancario                          as situacao_conta
        from transferegov.plano_acao_dado_bancario_minc
    ) t
    order by
        id_plano_acao, agencia, conta,
        (situacao_conta = 'Conta Ativa') desc,
        id_conta desc
),

controle as (
    select
        id_plano_acao::text                          as id_plano_acao,
        id_plano_acao_dado_bancario::text            as id_conta,
        count(*) filter (where status = 'ok')        as meses_ok,
        count(*) filter (where status = 'sem_dados') as meses_sem_dados,
        count(*) filter (where status = 'erro')      as meses_erro,
        -- CUIDADO: periodo_final provavelmente é TEXT. max() sobre texto é
        -- lexicográfico — correto só se o formato for YYYY-MM-DD. Confira com
        --   select periodo_final from bbagil.controle_extracao_bbagil_extrato limit 5;
        -- e, se for DD/MM/YYYY, troque por max(to_date(periodo_final,'DD/MM/YYYY')).
        max(periodo_final)                           as ultimo_periodo_extraido
    from bbagil.controle_extracao_bbagil_extrato
    group by 1, 2
),

classificado as (
    select
        p.programa,
        p.uf,
        p.id_plano_acao,
        b.id_conta,
        p.ente,
        p.inicio_vigencia,
        b.situacao_conta,
        -- 1 = a conta que a regra antiga (uma por plano: ativa primeiro, senão
        -- a de maior id) escolhia; 2+ = as que ela descartava sem deixar rastro
        row_number() over (
            partition by p.id_plano_acao
            order by (b.situacao_conta = 'Conta Ativa') desc, b.id_conta desc
        ) as ordem_conta,
        coalesce(c.meses_ok, 0)      as meses_ok,
        coalesce(c.meses_erro, 0)    as meses_erro,
        c.ultimo_periodo_extraido,
        -- partição mutuamente exclusiva, na ordem em que o pipeline descarta
        case
            when b.id_plano_acao is null              then '1_sem_registro_bancario'
            when b.agencia is null or b.conta is null
                 or b.conta = '0'                     then '2_sem_agencia_conta'
            when c.id_conta is null                   then '3_nunca_consultado'
            when coalesce(c.meses_ok, 0) = 0          then '4_consultado_sem_dados'
            else                                           '5_extraido'
        end as situacao_extracao
    from planos_escopo p
    left join banco    b on b.id_plano_acao = p.id_plano_acao
    left join controle c on c.id_plano_acao = p.id_plano_acao
                        and c.id_conta      = b.id_conta
)

-- -----------------------------------------------------------------------------
-- 0a — Visão geral: quantas contas por situação, em cada programa
-- -----------------------------------------------------------------------------
select
    programa,
    situacao_extracao,
    count(*)                                  as contas,
    count(*) filter (where ordem_conta > 1)   as contas_secundarias,
    count(distinct id_plano_acao)             as planos,
    round(100.0 * count(*) / sum(count(*)) over (partition by programa), 2) as pct
from classificado
group by 1, 2
order by 1, 2;


-- -----------------------------------------------------------------------------
-- 0a2 — O achado: contas secundárias que a regra antiga nunca consultou
-- -----------------------------------------------------------------------------
-- Antes de 03/09/2026 isto era 100% de `3_nunca_consultado` entre as
-- `ordem_conta > 1`. Depois de rodar a DAG corrigida, tem que cair para ~0 —
-- o que sobrar é conta que o BB rejeitou, e aí a mensagem_erro do controle diz
-- por quê.
-- select programa, ordem_conta, situacao_extracao, count(*) as contas
-- from classificado
-- group by 1, 2, 3
-- order by 1, 2, 3;


-- =============================================================================
-- Os blocos 0b–0d abaixo reusam o mesmo CTE `classificado`. Para rodar cada um,
-- mantenha todo o WITH acima e SUBSTITUA apenas o SELECT final (0a) por ele.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0b — Onde estão as contas nunca extraídas (UF × situação)
-- -----------------------------------------------------------------------------
-- select programa, uf,
--        count(*) filter (where situacao_extracao = '5_extraido')      as extraidos,
--        count(*) filter (where situacao_extracao <> '5_extraido')     as nao_extraidos,
--        round(100.0 * count(*) filter (where situacao_extracao = '5_extraido')
--              / nullif(count(*), 0), 2)                               as pct_extraido
-- from classificado
-- group by 1, 2
-- order by 1, pct_extraido asc;


-- -----------------------------------------------------------------------------
-- 0c — situacao_conta: o pipeline não filtra por ela de propósito (conta
-- encerrada é onde mora o histórico de 2023-2024 da LPG). Aqui ela responde
-- quanto do ganho das contas secundárias é conta ativa em paralelo e quanto é
-- conta encerrada com movimento passado.
-- -----------------------------------------------------------------------------
-- select programa, situacao_conta, situacao_extracao, count(*) as contas
-- from classificado
-- group by 1, 2, 3
-- order by 1, 2, 3;


-- -----------------------------------------------------------------------------
-- 0d — Erros de extração: contas que a gente tentou e falhou (recuperáveis)
-- -----------------------------------------------------------------------------
-- select programa, uf, ente, id_plano_acao, id_conta, ordem_conta,
--        meses_ok, meses_erro, ultimo_periodo_extraido
-- from classificado
-- where meses_erro > 0
-- order by meses_erro desc
-- limit 100;


-- =============================================================================
-- VERIFICAÇÃO — as 5 situações têm que somar o total de CONTAS em escopo, e os
-- planos distintos têm que bater com o total de planos. Se não somar, o CASE
-- não é uma partição (ou o `banco` voltou a duplicar conta).
-- =============================================================================
-- select
--     (select count(*) from classificado)                                   as total_classificado,
--     (select count(distinct id_plano_acao) from classificado)              as planos_classificados,
--     (select count(*) from transferegov.plano_acao_minc
--      where id_programa::text in ('46','47','60','61','62'))               as total_escopo;
