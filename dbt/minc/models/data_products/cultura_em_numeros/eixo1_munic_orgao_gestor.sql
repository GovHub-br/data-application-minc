-- Gold do Eixo 1, frente MUNIC: o tipo de órgão gestor da cultura em cada
-- edição, em três grupos (gráfico 4 do MUNIC.R). Fora 'Recusa'.
--
-- O agrupamento é diferente do da ESTADIC (eixo1_estadic_painel_uf) porque os dois
-- scripts R são diferentes, e cada gold segue o seu. O MUNIC.R agrupa no gráfico 4,
-- não no painel, não põe "indireta" no grupo exclusivo e chama o segundo grupo de
-- 'Secretaria em Conjunto (Ex: Educação e Cultura)'. O ESTADIC.R agrupa no painel,
-- põe "indireta" junto com exclusiva e fundação e chama o segundo grupo de
-- 'Secretaria Conjunta'. Aqui, 'Órgão da administração indireta' cai em 'Setor
-- Subordinado / Outros': são 119 municípios em 2014, 114 em 2018 e 114 em 2021
-- (11/11/2026).
with

    agrupado as (
        select
            ano,
            case
                when tipo_orgao_gestor ~* 'exclusiva|fundação'
                then 'Secretaria Exclusiva / Fundação'
                when tipo_orgao_gestor ~* 'conjunto'
                then 'Secretaria em Conjunto (Ex: Educação e Cultura)'
                else 'Setor Subordinado / Outros'
            end as tipo_orgao_gestor
        from {{ ref("eixo1_munic_painel_municipio") }}
        where
            tipo_orgao_gestor is not null
            and tipo_orgao_gestor not in ('Recusa', 'Sem Informação')
    ),

    contagem as (
        select ano, tipo_orgao_gestor, count(*) as quantidade_municipios
        from agrupado
        group by ano, tipo_orgao_gestor
    )

select
    ano,
    tipo_orgao_gestor,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
