-- Bronze transferegov — cópia fiel de transferegov.relatorios_gestao,
-- como a ponte staging_para_postgres_relatorios_gestao_dag grava (ADR 0011).
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_relatorios_gestao.
select * from {{ source("transferegov", "relatorios_gestao") }}  -- noqa: AM04
