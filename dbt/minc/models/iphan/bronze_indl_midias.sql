-- Bronze iphan — cópia fiel da coleção 357, "Imagens, sons e vídeos", de
-- indl.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_indl_midias. Sem cast,
-- filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção segue em
-- `metadata`, como a API o devolveu.
select * from {{ source("iphan", "indl_midias") }}  -- noqa: AM04
