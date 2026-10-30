-- Silver SALIC — silver_sac_vproponenteporarea (ex-sac__vproponenteporarea).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vproponenteporarea, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 1 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("area") }} as area, {{ bronze_texto("cgccpf") }} as cgccpf, _fatia
from {{ source("salic", "bronze_sac_vproponenteporarea") }}
