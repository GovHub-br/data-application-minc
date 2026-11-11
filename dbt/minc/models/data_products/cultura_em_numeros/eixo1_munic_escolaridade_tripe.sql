-- Gold do Eixo 1, frente MUNIC: a escolaridade do gestor nos municípios com e
-- sem o tripé completo (plano, conselho e fundo) em 2021 (gráfico 5 do MUNIC.R).
-- A proporção é dentro de cada grupo de tripé.
--
-- O tripé vem pronto do painel (tem_tripe_completo). Quem tem item vazio e nenhum
-- 'Não' fica fora da contagem e do total do grupo; o R o contaria como sem tripé.
-- Em 11/11/2026 nenhum município de 2021 tem item do tripé vazio.
with

    base as (
        select gestor_escolaridade_agrupada, tem_tripe_completo
        from {{ ref("eixo1_munic_painel_municipio") }}
        where
            ano = 2021
            and gestor_escolaridade_agrupada is not null
            and gestor_escolaridade_agrupada <> 'Sem Informação'
            and tem_tripe_completo is not null
    ),

    contagem as (
        select
            gestor_escolaridade_agrupada,
            case
                when tem_tripe_completo = 'Sim'
                then 'Com Tripé Completo'
                else 'Sem Tripé Completo'
            end as tripe_completo,
            count(*) as quantidade_municipios
        from base
        group by 1, 2
    )

select
    tripe_completo,
    gestor_escolaridade_agrupada,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by tripe_completo) as proporcao
from contagem
