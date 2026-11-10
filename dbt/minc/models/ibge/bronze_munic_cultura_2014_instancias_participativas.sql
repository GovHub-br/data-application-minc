-- Bronze ibge — cópia fiel de seeds.ibge_munic_cultura_2014_instancias_participativas,
-- como a ibge_perfil_dag grava
-- a aba "Instâncias participativas" da MUNIC 2014, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_munic_cultura_2014_instancias_participativas.
select * from {{ source("ibge", "munic_cultura_2014_instancias_participativas") }}  -- noqa: AM04
