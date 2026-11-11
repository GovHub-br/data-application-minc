-- Bronze ibge_sidra — cópia fiel de ibge_sidra.resultados, como a
-- ibge_agregados_dag grava (task extrair_resultados, só para os agregados da
-- Variable ibge_config).
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_resultados.
select * from {{ source("ibge_sidra", "resultados") }}  -- noqa: AM04
