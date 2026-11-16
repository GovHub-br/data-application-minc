-- Gold do Eixo 1, frente MUNIC: o número médio de membros dos conselhos
-- municipais de cultura por região em 2021 (gráfico 18 do MUNIC.R). Conta só os
-- conselhos com quantidade informada entre 1 e 149 membros, como no R. A média
-- sai sem arredondar; o gráfico do R mostra uma casa decimal.
select regiao, count(*) as quantidade_municipios, avg(cons_qtd_membros) as media_membros
from {{ ref("eixo1_munic_painel_municipio") }}
where
    ano = 2021
    and tem_conselho = 'Sim'
    and cons_qtd_membros > 0
    and cons_qtd_membros < 150
group by regiao
