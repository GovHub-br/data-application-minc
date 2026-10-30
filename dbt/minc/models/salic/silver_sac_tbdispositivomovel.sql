-- Silver SALIC — silver_sac_tbdispositivomovel (ex-sac__tbdispositivomovel).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbdispositivomovel, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
-- idregistration, o token de registro do aparelho no serviço de notificação,
-- fica só na Bronze: é credencial de envio.
select
    {{ bronze_timestamp("dtregistration") }} as dtregistration,
    {{ bronze_texto("nrcpf") }} as nrcpf,
    {{ bronze_timestamp("dtacesso") }} as dtacesso,
    {{ bronze_inteiro("iddispositivomovel") }} as iddispositivomovel,
    _fatia
from {{ source("salic", "bronze_sac_tbdispositivomovel") }}
