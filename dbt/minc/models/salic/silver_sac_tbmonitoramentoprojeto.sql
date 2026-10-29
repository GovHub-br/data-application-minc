-- Silver SALIC — silver_sac_tbmonitoramentoprojeto (ex-sac__tbmonitoramentoprojeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbmonitoramentoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 23 colunas: 21 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmonitoramentoprojeto") }} as idmonitoramentoprojeto,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("nrmonitoramento") }} as nrmonitoramento,
    {{ bronze_inteiro("tpnotificacao") }} as tpnotificacao,
    {{ bronze_timestamp("dtmonitoramento") }} as dtmonitoramento,
    {{ bronze_timestamp("dtliberacaoexecucao") }} as dtliberacaoexecucao,
    {{ bronze_numerico("vlhomologado") }} as vlhomologado,
    {{ bronze_numerico("vlreadequado") }} as vlreadequado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_numerico("vlperccaptado") }} as vlperccaptado,
    {{ bronze_numerico("vlrecebidodeoutroprojeto") }} as vlrecebidodeoutroprojeto,
    {{ bronze_numerico("vltransferidoparaoutroprojeto") }}
    as vltransferidoparaoutroprojeto,
    {{ bronze_numerico("vlcomprovado") }} as vlcomprovado,
    {{ bronze_numerico("vlperccomprovado") }} as vlperccomprovado,
    {{ bronze_numerico("vlsaldocontacaptacao") }} as vlsaldocontacaptacao,
    {{ bronze_numerico("vlsaldocontamovimento") }} as vlsaldocontamovimento,
    {{ bronze_numerico("vltransferidocontamovimento") }} as vltransferidocontamovimento,
    {{ bronze_numerico("vlutilizado") }} as vlutilizado,
    {{ bronze_numerico("vlpercutilizado") }} as vlpercutilizado,
    {{ bronze_numerico("vltotaldebitado") }} as vltotaldebitado,
    {{ bronze_texto("sisituacao") }} as sisituacao,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("salic", "bronze_sac_tbmonitoramentoprojeto") }}
