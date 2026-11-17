-- Bronze iphan — cópia fiel da coleção 1327, "11 - Imagem, som e vídeo", de
-- inrc.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_inrc_audiovisuais. Sem
-- cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "inrc_audiovisuais") }}  -- noqa: AM04
