-- Silver SALIC — silver_sac_vcartas (ex-sac__vcartas).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vcartas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 6 tipadas, 13 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_timestamp("dtcarta") }} as dtcarta,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_texto("numero") }} as numero,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("ano") }} as ano,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("processo") }} as processo,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("responsavel") }} as responsavel,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_inteiro("cep") }} as cep,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("unidadeanalise") }} as unidadeanalise,
    _fatia
from {{ source("salic", "bronze_sac_vcartas") }}
