-- Gold do Eixo 1, frente SNC: os componentes do sistema de cultura entre os
-- entes que aderiram ao SNC, nos municípios e nos estados com o DF. É a Tabela
-- 1.2 da publicação (seção 5 do snc_figuras_tabelas.R), com os rótulos e os
-- nomes de coluna do CSV dela.
--
-- Como no R: o denominador são os aderentes ('Publicado no DOU') da esfera, e
-- cada componente conta quem o tem com situação 'Concluída'. A adesão entra
-- como sexta linha, com quantidade igual ao total e proporção 1.
with

    aderentes as (
        select esfera, cod_ibge, componente, tem_componente
        from {{ ref("eixo1_snc_componente_ente") }}
        where aderente
    ),

    total as (
        select esfera, count(distinct cod_ibge) as total_aderentes
        from aderentes
        group by esfera
    ),

    contagem as (
        select esfera, componente, count(*) filter (where tem_componente) as quantidade
        from aderentes
        group by esfera, componente
        union all
        select
            esfera, 'Adesão publicada no DOU' as componente, total_aderentes as quantidade
        from total
    ),

    rotulado as (
        select
            c.quantidade,
            t.total_aderentes,
            case
                c.esfera
                when 'municipal'
                then 'Municípios'
                when 'estadual'
                then 'Estados e DF'
            end as esfera,
            case
                c.componente
                when 'Lei do sistema'
                then 'Lei do sistema local'
                when 'CPF completo'
                then 'Conselho, Plano e Fundo (CPF completo)'
                else c.componente
            end as componente,
            c.quantidade::numeric / t.total_aderentes as proporcao
        from contagem as c
        inner join total as t on c.esfera = t.esfera
    )

select esfera, componente, quantidade, total_aderentes, proporcao
from rotulado
