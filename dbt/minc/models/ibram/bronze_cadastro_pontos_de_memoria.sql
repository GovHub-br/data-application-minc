-- Bronze ibram — cópia fiel da coleção 179824, "Pontos de Memória", de
-- cadastro.museus.gov.br, que a tainacan_dag grava em
-- seeds.ibram_cadastro_pontos_de_memoria. Sem cast, filtro nem join (ADR 0010): tudo
-- continua text, e o formulário da coleção segue em `metadata`, como a API o
-- devolveu.
select * from {{ source("ibram", "cadastro_pontos_de_memoria") }}  -- noqa: AM04
