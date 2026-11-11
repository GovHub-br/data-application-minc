-- O painel e os conselhos da frente MUNIC do Eixo 1 têm uma linha por
-- município e edição. Uma linha a mais aqui é município contado duas vezes nos
-- gráficos (por exemplo, duas abas de 2014 com o mesmo código).
select 'eixo1_munic_painel_municipio' as modelo, cod_municipio, ano, count(*) as linhas
from {{ ref("eixo1_munic_painel_municipio") }}
group by cod_municipio, ano
having count(*) > 1
union all
select 'eixo1_munic_conselho_municipio' as modelo, cod_municipio, ano, count(*) as linhas
from {{ ref("eixo1_munic_conselho_municipio") }}
group by cod_municipio, ano
having count(*) > 1
