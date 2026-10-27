-- Bronze transferegov — cópia fiel de transferegov.plano_acao_minc, como a
-- api_planos_acao_dag grava. Sem cast, filtro nem join (ADR 0010). Quem lê é
-- silver_plano_acao_ente_ano.
select * from {{ source("transferegov", "plano_acao_minc") }}
