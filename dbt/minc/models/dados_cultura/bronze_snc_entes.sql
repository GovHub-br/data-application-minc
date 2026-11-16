-- Bronze dados_cultura — cópia fiel de seeds.dados_cultura_snc_entes, como a
-- dados_cultura_dag grava a lista de entes do SNC do portal de dados abertos.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text, com todas as
-- versões. Inclui as colunas de cliente_dados_cultura.DADOS_SENSIVEIS (dados
-- bancários, CNPJ, nomes e endereço), que não passam daqui: quem tipa, sem
-- elas, é silver_snc_entes.
select * from {{ source("dados_cultura", "snc_entes") }}  -- noqa: AM04
