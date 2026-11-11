-- Bronze ibge — cópia fiel de seeds.ibge_munic_cultura_2006_instrumentos_de_gestao,
-- como a ibge_perfil_dag grava
-- a aba "Instrumentos de gestão" da MUNIC 2006, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_munic_cultura_2006_instrumentos_de_gestao.
select * from {{ source("ibge", "munic_cultura_2006_instrumentos_de_gestao") }}  -- noqa: AM04
