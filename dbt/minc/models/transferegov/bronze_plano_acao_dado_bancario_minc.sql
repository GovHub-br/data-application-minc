-- Bronze transferegov — cópia fiel de transferegov.plano_acao_dado_bancario_minc,
-- como a ponte staging_para_postgres_plano_acao_dado_bancario_minc_dag grava (ADR 0011).
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_plano_acao_dado_bancario_minc.
select * from {{ source("transferegov", "plano_acao_dado_bancario_minc") }}  -- noqa: AM04
