-- Gold do Eixo 1, frente MUNIC: a proporção de municípios com o tripé completo
-- (plano, conselho e fundo) por UF em 2021 (gráfico 10 do MUNIC.R).
with

    base as (
        select
            regiao,
            uf,
            case
                when
                    (tem_plano = 'Sim')::int
                    + (tem_conselho = 'Sim')::int
                    + (tem_fundo = 'Sim')::int
                    = 3
                then 1
                when
                    tem_plano is not null
                    and tem_conselho is not null
                    and tem_fundo is not null
                then 0
            end as tripe_completo
        from {{ ref("eixo1_munic_painel_municipio") }}
        where ano = 2021 and uf <> 'Outro'
    )

select
    regiao,
    uf,
    sum(tripe_completo) as quantidade_municipios_tripe,
    count(*) as quantidade_municipios,
    avg(tripe_completo) as proporcao
from base
group by regiao, uf
