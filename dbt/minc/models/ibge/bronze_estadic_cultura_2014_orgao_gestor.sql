-- Bronze ibge — cópia fiel de seeds.ibge_estadic_cultura_2014_orgao_gestor, como a
-- ibge_perfil_dag grava
-- a aba "Órgão gestor" da ESTADIC 2014, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_cultura_2014_orgao_gestor.
select * from {{ source("ibge", "estadic_cultura_2014_orgao_gestor") }}  -- noqa: AM04
