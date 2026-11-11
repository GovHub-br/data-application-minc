-- Bronze ibge — cópia fiel de seeds.ibge_munic_cultura_2014_politicas_culturais, como a
-- ibge_perfil_dag grava
-- a aba "Políticas culturais" da MUNIC 2014, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_munic_cultura_2014_politicas_culturais.
select * from {{ source("ibge", "munic_cultura_2014_politicas_culturais") }}  -- noqa: AM04
