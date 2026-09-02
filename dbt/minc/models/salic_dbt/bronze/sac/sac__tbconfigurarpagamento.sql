-- Bronze SALIC — sac__tbconfigurarpagamento.
-- Origem: salic_bronze.sac__tbconfigurarpagamento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 6 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idconfigurarpagamento") }} as idconfigurarpagamento,
    {{ bronze_inteiro("nrdespachoinicial") }} as nrdespachoinicial,
    {{ bronze_inteiro("nrdespachofinal") }} as nrdespachofinal,
    {{ bronze_timestamp("dtconfiguracaopagamento") }} as dtconfiguracaopagamento,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbconfigurarpagamento") }}
