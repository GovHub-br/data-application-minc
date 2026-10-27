-- Bronze transferegov — cópia fiel de transferegov.anexos_relatorios,
-- como a ponte staging_para_postgres_anexos_relatorios_dag grava (ADR 0011).
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_anexos_relatorios.
select * from {{ source("transferegov", "anexos_relatorios") }}  -- noqa: AM04
