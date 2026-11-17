-- Bronze iphan — cópia fiel da coleção 65998, "Imagem, som e vídeo", de
-- bcr.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_bcr_midias_registro. Sem
-- cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "bcr_midias_registro") }}  -- noqa: AM04
