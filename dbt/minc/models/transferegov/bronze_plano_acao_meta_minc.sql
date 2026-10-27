-- Bronze transferegov — cópia fiel de transferegov.plano_acao_meta_minc,
-- como a ponte staging_para_postgres_plano_acao_meta_minc_dag grava (ADR 0011).
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_plano_acao_meta_minc.
select * from {{ source("transferegov", "plano_acao_meta_minc") }}  -- noqa: AM04
