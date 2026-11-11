-- Bronze ibge — cópia fiel de seeds.ibge_munic_cultura_2014_equipamentos, como a
-- ibge_perfil_dag grava
-- a aba "Equipamentos" da MUNIC 2014, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_munic_cultura_2014_equipamentos.
select * from {{ source("ibge", "munic_cultura_2014_equipamentos") }}  -- noqa: AM04
