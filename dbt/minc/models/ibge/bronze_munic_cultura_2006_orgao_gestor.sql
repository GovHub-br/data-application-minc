-- Bronze ibge — cópia fiel de seeds.ibge_munic_cultura_2006_orgao_gestor, como a
-- ibge_perfil_dag grava
-- a aba "Órgão gestor" da MUNIC 2006, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_munic_cultura_2006_orgao_gestor.
select * from {{ source("ibge", "munic_cultura_2006_orgao_gestor") }}  -- noqa: AM04
