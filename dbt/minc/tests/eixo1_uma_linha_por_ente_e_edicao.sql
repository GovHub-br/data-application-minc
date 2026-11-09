-- Os painéis do Eixo 1 têm uma linha por ente e edição: município na MUNIC, UF
-- na ESTADIC. Uma linha a mais aqui é ente contado duas vezes nos gráficos (por
-- exemplo, duas abas de 2014 com o mesmo código).
select
    'eixo1_munic_painel_municipio' as modelo,
    cod_municipio as codigo,
    ano,
    count(*) as linhas
from {{ ref("eixo1_munic_painel_municipio") }}
group by cod_municipio, ano
having count(*) > 1
union all
select
    'eixo1_munic_conselho_municipio' as modelo,
    cod_municipio as codigo,
    ano,
    count(*) as linhas
from {{ ref("eixo1_munic_conselho_municipio") }}
group by cod_municipio, ano
having count(*) > 1
union all
select 'eixo1_estadic_painel_uf' as modelo, cod_uf as codigo, ano, count(*) as linhas
from {{ ref("eixo1_estadic_painel_uf") }}
group by cod_uf, ano
having count(*) > 1
