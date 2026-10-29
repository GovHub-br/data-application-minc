-- Silver SALIC — silver_agentes_vufmunicipio (ex-agentes__vufmunicipio).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vufmunicipio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_inteiro("idmunicipio") }} as idmunicipio,
    {{ bronze_texto("municipio") }} as municipio,
    _fatia
from {{ source("salic", "bronze_agentes_vufmunicipio") }}
