-- Gold do Eixo 1, frente MUNIC: a proporção de municípios com biblioteca, museu,
-- teatro e cinema em cada edição (gráfico 11 do MUNIC.R). Só conta resposta
-- 'Sim' ou 'Não'.
with

    equipamentos as (
        select ano, 'Biblioteca Pública' as equipamento, equip_biblioteca as resposta
        from {{ ref("eixo1_munic_painel_municipio") }}
        union all
        select ano, 'Museu' as equipamento, equip_museu as resposta
        from {{ ref("eixo1_munic_painel_municipio") }}
        union all
        select
            ano, 'Teatro / Casa de Espetáculos' as equipamento, equip_teatro as resposta
        from {{ ref("eixo1_munic_painel_municipio") }}
        union all
        select ano, 'Cinema / Sala de Exibição' as equipamento, equip_cinema as resposta
        from {{ ref("eixo1_munic_painel_municipio") }}
    )

select
    ano,
    equipamento,
    count(*) filter (where resposta = 'Sim') as quantidade_municipios_sim,
    count(*) as quantidade_municipios,
    avg(case when resposta = 'Sim' then 1 else 0 end) as proporcao
from equipamentos
where resposta in ('Sim', 'Não')
group by ano, equipamento
