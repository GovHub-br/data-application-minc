-- Silver SALIC — silver_agentes_vperfil (ex-agentes__vperfil).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vperfil, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 4 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("perfil") }} as perfil,
    {{ bronze_inteiro("caracteristica") }} as caracteristica,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("salic", "bronze_agentes_vperfil") }}
