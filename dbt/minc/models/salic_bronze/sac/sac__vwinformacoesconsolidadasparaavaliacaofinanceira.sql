-- Bronze SALIC — sac__vwinformacoesconsolidadasparaavaliacaofinanceira.
-- Origem: salic_bronze.sac__vwinformacoesconsolidadasparaavaliacaofinanceira, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 2 tipadas, 18 mantidas como texto.
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
    {{ bronze_texto("dtenviodaprestacaocontas") }} as dtenviodaprestacaocontas,
    {{ bronze_texto("resultadoavaliacaoobjeto") }} as resultadoavaliacaoobjeto,
    {{ bronze_texto("qtempregosdiretos") }} as qtempregosdiretos,
    {{ bronze_texto("qtempregosindiretos") }} as qtempregosindiretos,
    {{ bronze_texto("qtempregosgerados") }} as qtempregosgerados,
    {{ bronze_texto("qtcomprovacao") }} as qtcomprovacao,
    {{ bronze_texto("qtnc_90") }} as qtnc_90,
    {{ bronze_texto("qtnc_95") }} as qtnc_95,
    {{ bronze_texto("qtnc_99") }} as qtnc_99,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_texto("vlcaptado") }} as vlcaptado,
    {{ bronze_texto("vlcomprovado") }} as vlcomprovado,
    _fatia
from {{ source("bronze_sac", "sac__vwinformacoesconsolidadasparaavaliacaofinanceira") }}
