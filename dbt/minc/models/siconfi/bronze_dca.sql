-- Bronze siconfi — cópia fiel de siconfi.dca, como a siconfi_bronze_dag
-- grava a partir do raw do MinIO, pelo Trino. Sem cast, filtro nem join
-- (ADR 0010): tudo continua varchar, e as repetições da extração também.
select * from {{ source("siconfi", "dca") }}  -- noqa: AM04
