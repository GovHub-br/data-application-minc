-- Bronze transferegov — cópia fiel de transferegov.programa_minc,
-- como a ponte staging_para_postgres_programa_minc_dag grava (ADR 0011).
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_programa_minc.
select * from {{ source("transferegov", "programa_minc") }}  -- noqa: AM04
