-- Bronze tesouro_transparente — cópia fiel de
-- seeds.tesouro_transparente_lrf_uniao_dcl, como a tesouro_transparente_dag
-- grava o CSV "Limites da LRF para a União: Dívida Consolidada Líquida".
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_lrf_uniao_dcl.
select * from {{ source("tesouro_transparente", "lrf_uniao_dcl") }}  -- noqa: AM04
