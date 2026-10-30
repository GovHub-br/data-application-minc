-- Silver SALIC — silver_agentes_vwcadastrarparecerista
-- (ex-agentes__vwcadastrarparecerista).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vwcadastrarparecerista, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 9 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_inteiro("idnome") }} as idnome,
    {{ bronze_inteiro("tiponome") }} as tiponome,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_inteiro("idendereco") }} as idendereco,
    {{ bronze_inteiro("tipoendereco") }} as tipoendereco,
    {{ bronze_inteiro("tipologradouro") }} as tipologradouro,
    {{ bronze_texto("logradouro") }} as logradouro,
    {{ bronze_texto("numero") }} as numero,
    {{ bronze_texto("bairro") }} as bairro,
    {{ bronze_texto("complemento") }} as complemento,
    {{ bronze_inteiro("cidade") }} as cidade,
    {{ bronze_inteiro("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("divulgarendereco") }} as divulgarendereco,
    {{ bronze_texto("correspondencia") }} as correspondencia,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("salic", "bronze_agentes_vwcadastrarparecerista") }}
