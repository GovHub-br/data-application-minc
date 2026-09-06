-- Bronze SALIC — sac__carregarecibocaptacaodanca2012.
-- Origem: salic_bronze.sac__carregarecibocaptacaodanca2012, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 1 tipadas, 15 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("operacao") }} as operacao,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_texto("numerorecibo") }} as numerorecibo,
    {{ bronze_texto("datarecibo") }} as datarecibo,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("cpfcnpj") }} as cpfcnpj,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("ddd") }} as ddd,
    {{ bronze_texto("telefone") }} as telefone,
    {{ bronze_texto("pfpj") }} as pfpj,
    _fatia
from {{ source("bronze_sac", "sac__carregarecibocaptacaodanca2012") }}
