-- O painel da ESTADIC tem as 27 UFs em cada edição, e o mapa de 2021 também. O
-- eixo1_uma_linha_por_ente_e_edicao pega a UF repetida; este pega a que falta.
-- Em 2014 e 2023, a primeira aba dos left joins (orgao_gestor e
-- primeira_infancia) decide que UFs entram: se ela perder uma, o painel perde a
-- linha sem erro, e todas as proporções daquela edição mudam.
--
-- A edição que vier vazia não aparece no group by, por isso a segunda parte
-- confere as seis edições de hoje. Uma edição nova já é conferida pela primeira
-- parte; entra na lista só para acusar se um dia vier vazia.
with

    painel as (select ano from {{ ref("eixo1_estadic_painel_uf") }}),

    edicoes_conhecidas as (
        select ano from unnest(array[2012, 2014, 2018, 2021, 2023, 2024]) as ano
    )

select 'eixo1_estadic_painel_uf' as modelo, ano, count(*) as linhas
from painel
group by ano
having count(*) <> 27
union all
select 'eixo1_estadic_painel_uf' as modelo, edicoes_conhecidas.ano, 0 as linhas
from edicoes_conhecidas
where not exists (select 1 from painel where painel.ano = edicoes_conhecidas.ano)
union all
select 'eixo1_estadic_mapa_uf_2021' as modelo, 2021 as ano, count(*) as linhas
from {{ ref("eixo1_estadic_mapa_uf_2021") }}
having count(*) <> 27
