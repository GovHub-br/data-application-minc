-- No gráfico 10, a proporção de cada UF tem de ser a razão das duas contagens:
-- as três contam os mesmos municípios, os de tripé conhecido. Volta a UF em que
-- isso não vale, como quando a proporção ignorava o tripé nulo e o total não.
select uf, quantidade_municipios_tripe, quantidade_municipios, proporcao
from {{ ref("eixo1_munic_tripe_uf") }}
where
    proporcao is null
    or abs(
        proporcao
        - quantidade_municipios_tripe::numeric / nullif(quantidade_municipios, 0)
    )
    > 1e-12
