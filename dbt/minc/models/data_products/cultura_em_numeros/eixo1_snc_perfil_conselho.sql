-- Gold do Eixo 1, frente SNC: o perfil dos conselhos municipais de cultura
-- instituídos por lei, isto é, com situação 'Concluída' (tabela da seção 10 do
-- SNC.R, Tabela_SNC_Perfil_Conselhos). Uma linha por característica e resposta,
-- inclusive as que têm zero, como na tabela do R. Resposta ausente na planilha
-- conta como 'Não informado', a coluna que a tabela do R tem para isso.
with

    conselhos as (
        select
            perfil_conselho_exclusivo,
            perfil_conselho_paritario,
            perfil_conselho_deliberativo,
            perfil_conselho_atuante
        from {{ ref("eixo1_snc_painel_municipio") }}
        where situacao_conselho = 'Concluída'
    ),

    respostas as (
        select
            'Conselho exclusivo de cultura?' as caracteristica,
            perfil_conselho_exclusivo as resposta
        from conselhos
        union all
        select
            'Conselho paritário?' as caracteristica, perfil_conselho_paritario as resposta
        from conselhos
        union all
        select
            'Conselho deliberativo?' as caracteristica,
            perfil_conselho_deliberativo as resposta
        from conselhos
        union all
        select
            'Realizou reunião entre 2025–2026 (atuante)?' as caracteristica,
            perfil_conselho_atuante as resposta
        from conselhos
    ),

    contagem as (
        select
            caracteristica,
            coalesce(resposta, 'Não informado') as resposta,
            count(*) as quantidade_municipios
        from respostas
        group by caracteristica, coalesce(resposta, 'Não informado')
    ),

    caracteristicas as (select distinct caracteristica from respostas),

    respostas_possiveis as (
        select 'Sim' as resposta
        union all
        select 'Não' as resposta
        union all
        select 'Não informado' as resposta
    ),

    total as (select count(*) as total_conselhos from conselhos),

    grade as (
        select c.caracteristica, r.resposta
        from caracteristicas as c
        cross join respostas_possiveis as r
    )

select
    g.caracteristica,
    g.resposta,
    t.total_conselhos,
    coalesce(c.quantidade_municipios, 0) as quantidade_municipios,
    coalesce(c.quantidade_municipios, 0)::numeric / t.total_conselhos as proporcao
from grade as g
cross join total as t
left join contagem as c on g.caracteristica = c.caracteristica and g.resposta = c.resposta
