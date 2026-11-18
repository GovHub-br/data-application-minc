-- Bronze ibge — cópia fiel de seeds.ibge_estadic_cultura_2014_recursos_humanos, como a
-- ibge_perfil_dag grava
-- a aba "Recursos humanos" da ESTADIC 2014, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_cultura_2014_recursos_humanos.
select * from {{ source("ibge", "estadic_cultura_2014_recursos_humanos") }}  -- noqa: AM04
