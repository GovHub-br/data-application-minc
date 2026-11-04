-- Silver SALIC — silver_sac_vnacional (ex-sac__vnacional).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vnacional, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 6 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idendereco") }} as idendereco,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("tipoendereco") }} as tipoendereco,
    {{ bronze_inteiro("tipologradouro") }} as tipologradouro,
    {{ bronze_texto("logradouro") }} as logradouro,
    {{ bronze_texto("numero") }} as numero,
    {{ bronze_texto("bairro") }} as bairro,
    {{ bronze_texto("complemento") }} as complemento,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_inteiro("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("status") }} as status,
    {{ bronze_texto("divulgar") }} as divulgar,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("salic", "bronze_sac_vnacional") }}
