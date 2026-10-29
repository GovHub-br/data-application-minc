-- Silver SALIC — silver_agentes_vwempresajuridicasocios
-- (ex-agentes__vwempresajuridicasocios).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vwempresajuridicasocios, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 3 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idempresajuridicasocios") }} as idempresajuridicasocios,
    {{ bronze_texto("nrcnpj") }} as nrcnpj,
    {{ bronze_texto("nmempresarial") }} as nmempresarial,
    {{ bronze_inteiro("tpsocio") }} as tpsocio,
    {{ bronze_texto("dstiposocio") }} as dstiposocio,
    {{ bronze_texto("nrcnpjcpfsocio") }} as nrcnpjcpfsocio,
    {{ bronze_texto("nmsocio") }} as nmsocio,
    {{ bronze_data("dtinclusaosociedade") }} as dtinclusaosociedade,
    {{ bronze_texto("cdqualificacao") }} as cdqualificacao,
    {{ bronze_texto("dsqualificacaosocio") }} as dsqualificacaosocio,
    _fatia
from {{ source("salic", "bronze_agentes_vwempresajuridicasocios") }}
