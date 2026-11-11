-- Gold do Eixo 1, frente MUNIC: a proporção de municípios com o tripé completo
-- (plano, conselho e fundo) por UF em 2021 (gráfico 10 do MUNIC.R).
--
-- O tripé vem pronto do painel (tem_tripe_completo). As duas contagens e a
-- proporção contam os mesmos municípios: os de tripé conhecido. Quem tem item
-- vazio e nenhum 'Não' fica fora das três; o R o contaria como sem tripé. Em
-- 11/11/2026 nenhum município de 2021 tem item do tripé vazio.
select
    regiao,
    uf,
    count(*) filter (where tem_tripe_completo = 'Sim') as quantidade_municipios_tripe,
    count(tem_tripe_completo) as quantidade_municipios,
    avg((tem_tripe_completo = 'Sim')::int) as proporcao
from {{ ref("eixo1_munic_painel_municipio") }}
where ano = 2021 and uf <> 'Outro'
group by regiao, uf
