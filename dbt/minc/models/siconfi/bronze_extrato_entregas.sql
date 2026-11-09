-- Bronze siconfi — cópia fiel de siconfi.extrato_entregas, como a siconfi_bronze_dag
-- grava a partir do raw do MinIO, pelo Trino. Sem cast, filtro nem join
-- (ADR 0010): tudo continua varchar, e as repetições da extração também.
select * from {{ source("siconfi", "extrato_entregas") }}  -- noqa: AM04
