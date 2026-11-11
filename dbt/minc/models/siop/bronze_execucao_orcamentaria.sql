-- Bronze siop — cópia fiel de seeds.siop_execucao_orcamentaria, como a
-- siop_execucao_dag grava a execução da LOA por exercício.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_execucao_orcamentaria.
select * from {{ source("siop", "execucao_orcamentaria") }}  -- noqa: AM04
