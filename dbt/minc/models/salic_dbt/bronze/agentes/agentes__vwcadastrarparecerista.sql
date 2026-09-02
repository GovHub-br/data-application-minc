-- Bronze SALIC — agentes__vwcadastrarparecerista.
-- Origem: salic_bronze.agentes__vwcadastrarparecerista, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 10 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_inteiro("idnome") }} as idnome,
    {{ bronze_texto("tiponome") }} as tiponome,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_inteiro("idendereco") }} as idendereco,
    {{ bronze_inteiro("tipoendereco") }} as tipoendereco,
    {{ bronze_inteiro("tipologradouro") }} as tipologradouro,
    {{ bronze_texto("logradouro") }} as logradouro,
    {{ bronze_inteiro("numero") }} as numero,
    {{ bronze_texto("bairro") }} as bairro,
    {{ bronze_texto("complemento") }} as complemento,
    {{ bronze_inteiro("cidade") }} as cidade,
    {{ bronze_inteiro("uf") }} as uf,
    {{ bronze_inteiro("cep") }} as cep,
    {{ bronze_texto("divulgarendereco") }} as divulgarendereco,
    {{ bronze_texto("correspondencia") }} as correspondencia,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("bronze_agentes", "agentes__vwcadastrarparecerista") }}
