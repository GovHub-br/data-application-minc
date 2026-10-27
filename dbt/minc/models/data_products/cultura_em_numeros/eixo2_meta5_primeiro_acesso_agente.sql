{{ config(materialized="table") }}

-- Gold — grão fino da Meta 5: uma linha por (agente × mecanismo), dizendo
-- quando ele entrou naquele mecanismo e se aquela entrada foi a porta de
-- entrada dele no fomento federal como um todo. As golds de consumo da meta
-- derivam daqui, e é aqui que se olha para auditar um agente específico.
--
-- É Gold, e não Intermediate, porque não cruza base nenhuma: aplica a
-- definição de novo entrante (porta de entrada e desempate por mecanismo),
-- que é regra de negócio da meta (ADR 0010). Lê eixo2_meta5_evento_fomento,
-- Gold do mesmo produto.
--
-- A DEFINIÇÃO QUE IMPORTA: novo entrante é comparação CROSS-MECANISMO. O
-- primeiro acesso do agente é o menor evento dele em QUALQUER mecanismo
-- (ver data_primeiro_acesso_geral, particionada só por documento). Quem
-- captou Rouanet em 2010 e recebeu LPG em 2023 NÃO é novo entrante da LPG —
-- é justamente o caso que a pergunta quer separar, e que a versão anterior
-- deste indicador não conseguia enxergar porque só tinha as duas fontes do
-- fundo a fundo, ambas começando em 2023.
--
-- EMPATE: se o menor evento do agente cai no mesmo dia em dois mecanismos,
-- ambos seriam marcados como porta de entrada e o agente contaria duas vezes
-- na série anual. O desempate é determinístico por ordem de mecanismo, a
-- política mais antiga primeiro — ROUANET (1991) < AUDIOVISUAL (Lei 8.685/93)
-- < FSA (2006) < LAB (2020) < LPG (2022) < PNAB (2023) — via ROW_NUMBER,
-- garantindo exatamente uma porta de entrada por agente.
with
    por_agente_mecanismo as (
        select
            beneficiario_documento,
            programa_fomento,
            tipo_pessoa,
            min(data_evento) as data_primeiro_acesso_mecanismo,
            max(data_evento) as data_ultimo_acesso_mecanismo,
            min(ano_inicio_observacao) as ano_inicio_observacao,
            count(*) as qtd_eventos,
            sum(valor) as valor_total
        from {{ ref("eixo2_meta5_evento_fomento") }}
        group by beneficiario_documento, programa_fomento, tipo_pessoa
    ),

    com_geral as (
        select
            *,
            min(data_primeiro_acesso_mecanismo) over (
                partition by beneficiario_documento
            ) as data_primeiro_acesso_geral,
            count(*) over (partition by beneficiario_documento) as qtd_mecanismos_agente,
            row_number() over (
                partition by beneficiario_documento
                order by
                    data_primeiro_acesso_mecanismo,
                    case
                        programa_fomento
                        when 'ROUANET'
                        then 1
                        when 'AUDIOVISUAL'
                        then 2
                        when 'FSA'
                        then 3
                        when 'LAB'
                        then 4
                        when 'LPG'
                        then 5
                        when 'PNAB'
                        then 6
                    end
            ) as ordem_entrada
        from por_agente_mecanismo
    )

select
    beneficiario_documento,
    programa_fomento,
    tipo_pessoa,
    data_primeiro_acesso_mecanismo,
    data_ultimo_acesso_mecanismo,
    extract(year from data_primeiro_acesso_mecanismo)::int as ano_entrada_mecanismo,
    data_primeiro_acesso_geral,
    extract(year from data_primeiro_acesso_geral)::int as ano_primeiro_acesso_geral,
    qtd_mecanismos_agente,
    qtd_eventos,
    valor_total,
    -- exatamente uma linha por agente tem TRUE: o mecanismo pelo qual ele
    -- entrou no fomento federal
    (ordem_entrada = 1) as eh_porta_de_entrada,
    case when ordem_entrada = 1 then 'Sim' else 'Não' end as categoria_primeiro_acesso,
    -- no primeiro ano observável de um mecanismo não existe passado para
    -- comparar; todo mundo parece estreante por construção. A gold anual usa
    -- esta marca para não vender artefato de janela como resultado.
    (
        extract(year from data_primeiro_acesso_mecanismo)::int = ano_inicio_observacao
    ) as entrada_em_ano_censurado
from com_geral
