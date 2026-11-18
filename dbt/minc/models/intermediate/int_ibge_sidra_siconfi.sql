-- Intermediate — os municípios do SICONFI com a população do Censo 2022.
--
-- Cruza duas fontes e não decide nada (ADR 0010, regra 2; ADR 0015, decisão 3):
-- • siconfi: cada município de silver_entes (esfera 'M'), com nome, sigla da
-- UF, região, se é capital e a população que o SICONFI informa;
-- • ibge_sidra: a população residente do Censo 2022 (silver_resultados,
-- agregado 4709, variável 93, nível N6 = município, período 2022), pelo código
-- IBGE de 7 dígitos.
--
-- Município sem linha no Censo (criado depois de 2022) fica com a população do
-- Censo nula. O porte (faixas da PNAS) é regra, e fica na Gold.
-- GRÃO: uma linha por município de silver_entes (cod_ibge).
with
    censo_2022 as (
        select localidade_id as cod_ibge, valor as populacao_censo_2022
        from {{ ref("silver_resultados") }}
        where
            agregado_id = 4709 and variavel_id = 93 and nivel = 'N6' and periodo = '2022'
    )

select
    ente.cod_ibge,
    ente.ente,
    ente.sigla_uf,
    ente.regiao,
    ente.capital,
    ente.populacao as populacao_siconfi,
    ente.exercicio as exercicio_populacao_siconfi,
    censo_2022.populacao_censo_2022
from {{ ref("silver_entes") }} as ente
left join censo_2022 on ente.cod_ibge = censo_2022.cod_ibge
where ente.esfera = 'M'
