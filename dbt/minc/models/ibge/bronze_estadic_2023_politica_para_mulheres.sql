-- Bronze ibge — cópia fiel de seeds.ibge_estadic_2023_politica_para_mulheres, como a
-- ibge_perfil_dag grava
-- a aba "Política para Mulheres" da ESTADIC 2023.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_2023_politica_para_mulheres.
select * from {{ source("ibge", "estadic_2023_politica_para_mulheres") }}  -- noqa: AM04
