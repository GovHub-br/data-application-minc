-- Bronze SALIC — sac__tbpareceristavaloranalisado.
-- Origem: salic_bronze.sac__tbpareceristavaloranalisado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_data("dtassinatura") }} as dtassinatura,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_texto("tipo") }} as tipo,
    _fatia
from {{ source("bronze_sac", "sac__tbpareceristavaloranalisado") }}
