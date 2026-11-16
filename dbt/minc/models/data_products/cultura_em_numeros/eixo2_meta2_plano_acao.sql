-- Gold — Meta 2 do PNAB (execução financeira), gold-base: uma linha por plano de
-- ação dos programas da PNAB no TransfereGov, em qualquer situação.
--
-- O escopo é regra de negócio (ADR 0015) e mora aqui: os programas 60, 61 e 62
-- (PNAB ciclo 1, 2023) e 111 e 112 (PNAB 2025). O porte vem do programa, pela
-- regra do painel da Meta 2 (aba tab-meta2 de docs/index.html): 60 é estado ou
-- DF, 61 é município com repasse a partir de R$ 360 mil, 62 abaixo disso. O 111,
-- de estados em 2025, segue o 60. O 112 junta os dois portes de município e fica
-- sem porte. O DF entra no programa de estados e no de municípios; no de
-- municípios, é a linha com tipo_ente = 'ESTADO'.
--
-- Dos relatórios de gestão saem só fatos: quantos o plano tem, a situação do
-- mais recente por data_e_hora e se algum foi APROVADO. O valor executado, o
-- declarado no relatório ou o pago pelo BB Ágil, fica de fora até a issue da
-- Meta 2 decidir qual medida vale, quais situações contam e qual relatório vale
-- quando há mais de um.
with

    plano as (
        select
            id_plano_acao,
            codigo_plano_acao,
            id_programa,
            tipo_ente,
            id_ente_recebedor_plano_acao,
            uf_ente_recebedor_plano_acao,
            cod_ibge_uf,
            cod_ibge_municipio,
            situacao_plano_acao,
            data_inicio_vigencia_plano_acao,
            data_fim_vigencia_plano_acao,
            valor_total_repasse_plano_acao,
            case
                when id_programa in (60, 111)
                then 'ESTADO_DF'
                when id_programa = 61
                then 'MUNICIPIO_GE_360K'
                when id_programa = 62
                then 'MUNICIPIO_LT_360K'
                when id_programa = 112
                then 'MUNICIPIO_SEM_PORTE'
            end as porte_ente
        from {{ ref("silver_plano_acao_minc") }}
        where id_programa in (60, 61, 62, 111, 112)
    ),

    programa as (
        select id_programa, ano_programa, nome_programa
        from {{ ref("silver_programa_minc") }}
    ),

    -- O mais recente é o de maior data_e_hora; o id_relatorio_gestao só desempata,
    -- para o resultado não depender da ordem de leitura. Em 11/11/2026 nenhum plano
    -- tem dois relatórios com a mesma data_e_hora.
    relatorio as (
        select
            id_plano_acao,
            id_relatorio_gestao,
            situacao_relatorio_gestao,
            data_e_hora_relatorio_gestao,
            row_number() over (
                partition by id_plano_acao
                order by
                    data_e_hora_relatorio_gestao desc nulls last, id_relatorio_gestao desc
            ) as ordem_recente,
            count(*) over (partition by id_plano_acao) as quantidade_relatorios,
            bool_or(situacao_relatorio_gestao = 'APROVADO') over (
                partition by id_plano_acao
            ) as algum_aprovado
        from {{ ref("silver_relatorios_gestao") }}
    ),

    relatorio_recente as (
        select
            id_plano_acao,
            id_relatorio_gestao,
            situacao_relatorio_gestao,
            data_e_hora_relatorio_gestao,
            quantidade_relatorios,
            algum_aprovado
        from relatorio
        where ordem_recente = 1
    )

select
    plano.id_plano_acao,
    plano.codigo_plano_acao,
    plano.id_programa,
    programa.ano_programa,
    programa.nome_programa,
    plano.porte_ente,
    plano.tipo_ente,
    plano.id_ente_recebedor_plano_acao,
    plano.uf_ente_recebedor_plano_acao as sigla_uf,
    plano.cod_ibge_uf,
    plano.cod_ibge_municipio,
    plano.situacao_plano_acao,
    plano.data_inicio_vigencia_plano_acao,
    plano.data_fim_vigencia_plano_acao,
    plano.valor_total_repasse_plano_acao,
    recente.id_relatorio_gestao as id_relatorio_gestao_recente,
    recente.situacao_relatorio_gestao as situacao_relatorio_gestao_recente,
    recente.data_e_hora_relatorio_gestao as data_e_hora_relatorio_gestao_recente,
    -- NULL quando o plano não tem relatório (regra transversal 1 do ADR 0010).
    recente.algum_aprovado as flag_relatorio_aprovado,
    -- Plano sem relatório tem zero relatórios: a contagem é um fato, não ausência.
    coalesce(recente.quantidade_relatorios, 0) as quantidade_relatorios_gestao
from plano
left join programa on plano.id_programa = programa.id_programa
left join relatorio_recente as recente on plano.id_plano_acao = recente.id_plano_acao
