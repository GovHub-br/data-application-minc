-- Bronze senado — cópia fiel de senado.legislacao_detalhe_secoes, como a
-- senado_legislacao_dag grava a partir do raw do MinIO, pelo Trino. Sem cast,
-- filtro nem join (ADR 0010): tudo continua varchar, e as repetições da
-- extração também.
select * from {{ source("senado", "legislacao_detalhe_secoes") }}  -- noqa: AM04
