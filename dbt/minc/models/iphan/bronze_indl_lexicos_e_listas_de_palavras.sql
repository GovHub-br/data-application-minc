-- Bronze iphan — cópia fiel da coleção 90288, "Léxicos e Listas de Palavras", de
-- indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_lexicos_e_listas_de_palavras. Sem cast, filtro nem join (ADR
-- 0010): tudo continua text, e o formulário da coleção segue em `metadata`, como a
-- API o devolveu.
select * from {{ source("iphan", "indl_lexicos_e_listas_de_palavras") }}  -- noqa: AM04
