-- Bronze ibge — cópia fiel de seeds.ibge_estadic_2012_conselhos_e_fundos, como a
-- ibge_perfil_dag grava
-- a aba "Conselhos e fundos" da ESTADIC 2012.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_2012_conselhos_e_fundos.
select * from {{ source("ibge", "estadic_2012_conselhos_e_fundos") }}  -- noqa: AM04
