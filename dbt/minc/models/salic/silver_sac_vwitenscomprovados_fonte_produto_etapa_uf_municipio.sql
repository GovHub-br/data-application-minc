-- Silver SALIC — silver_sac_vwitenscomprovados_fonte_produto_etapa_uf_municipio (ex-sac__vwitenscomprovados_fonte_produto_etapa_uf_municipio).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwitenscomprovados_fonte_produto_etapa_uf_municipio, onde
-- tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 8 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idplanilhaaprovacao") }} as idplanilhaaprovacao,
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_inteiro("idplanilhaetapa") }} as idplanilhaetapa,
    {{ bronze_texto("descetapa") }} as descetapa,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_inteiro("idplanilhaitens") }} as idplanilhaitens,
    {{ bronze_texto("descitem") }} as descitem,
    {{ bronze_texto("qtitem") }} as qtitem,
    {{ bronze_inteiro("nrocorrencia") }} as nrocorrencia,
    {{ bronze_numerico("vlunitario") }} as vlunitario,
    {{ bronze_texto("total") }} as total,
    {{ bronze_numerico("vlcomprovado") }} as vlcomprovado,
    {{ bronze_texto("comprovacaovalidada") }} as comprovacaovalidada,
    {{ bronze_texto("tpcusto") }} as tpcusto,
    {{ bronze_texto("stitemavaliado") }} as stitemavaliado,
    _fatia
from
    {{ source("salic", "bronze_sac_vwitenscomprovados_fonte_produto_etapa_uf_municipio") }}
