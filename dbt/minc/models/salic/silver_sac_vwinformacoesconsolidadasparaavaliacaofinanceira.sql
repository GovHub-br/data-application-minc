-- Silver SALIC — silver_sac_vwinformacoesconsolidadasparaavaliacaofinanceira
-- (ex-sac__vwinformacoesconsolidadasparaavaliacaofinanceira).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwinformacoesconsolidadasparaavaliacaofinanceira, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 12 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dtenviodaprestacaocontas") }} as dtenviodaprestacaocontas,
    {{ bronze_texto("resultadoavaliacaoobjeto") }} as resultadoavaliacaoobjeto,
    {{ bronze_inteiro("qtempregosdiretos") }} as qtempregosdiretos,
    {{ bronze_inteiro("qtempregosindiretos") }} as qtempregosindiretos,
    {{ bronze_inteiro("qtempregosgerados") }} as qtempregosgerados,
    {{ bronze_inteiro("qtcomprovacao") }} as qtcomprovacao,
    {{ bronze_inteiro("qtnc_90") }} as qtnc_90,
    {{ bronze_inteiro("qtnc_95") }} as qtnc_95,
    {{ bronze_inteiro("qtnc_99") }} as qtnc_99,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_numerico("vlcomprovado") }} as vlcomprovado,
    _fatia
from {{ source("salic", "bronze_sac_vwinformacoesconsolidadasparaavaliacaofinanceira") }}
