-- Todo estado, o DF e todo município de silver_entes têm sigla_uf, e no
-- município ela é a mesma uf que o SICONFI devolve. O SICONFI grava uf = 'BR'
-- nos estados e no DF, então a sigla deles sai do código IBGE; este teste
-- confere o mapa contra os 5.570 municípios, que trazem a sigla de verdade.
-- Devolve os entes fora disso.
select cod_ibge, ente, esfera, uf, sigla_uf
from {{ ref("silver_entes") }}
where
    esfera in ('E', 'D', 'M') and (sigla_uf is null or (esfera = 'M' and sigla_uf <> uf))
