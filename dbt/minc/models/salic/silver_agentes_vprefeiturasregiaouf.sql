-- Silver SALIC — silver_agentes_vprefeiturasregiaouf (ex-agentes__vprefeiturasregiaouf).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vprefeiturasregiaouf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 0 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("perfil") }} as perfil,
    {{ bronze_texto("cnpj") }} as cnpj,
    {{ bronze_texto("prefeitura") }} as prefeitura,
    {{ bronze_texto("municipio") }} as municipio,
    _fatia
from {{ source("salic", "bronze_agentes_vprefeiturasregiaouf") }}
