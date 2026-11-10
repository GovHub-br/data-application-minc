-- O código do município nas golds-base da MUNIC é o de 2021, com 7 dígitos. Um
-- código de 6 dígitos é município de 2006 que não casou com o cadastro de 2021:
-- o painel o deixaria passar com nome nulo (o coalesce volta para o código da
-- edição).
select 'eixo1_munic_painel_municipio' as modelo, cod_municipio, ano
from {{ ref("eixo1_munic_painel_municipio") }}
where cod_municipio !~ '^[0-9]{7}$'
union all
select 'eixo1_munic_conselho_municipio' as modelo, cod_municipio, ano
from {{ ref("eixo1_munic_conselho_municipio") }}
where cod_municipio !~ '^[0-9]{7}$'
