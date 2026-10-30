-- Silver SALIC — silver_sac_tbinadimplente (ex-sac__tbinadimplente).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbinadimplente, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 9 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idinadimplente") }} as idinadimplente,
    {{ bronze_texto("nrcnpjcpf") }} as nrcnpjcpf,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dtinadimplencia") }} as dtinadimplencia,
    {{ bronze_texto("cdsituacaoanteriorinadimplencia") }}
    as cdsituacaoanteriorinadimplencia,
    {{ bronze_texto("dsprovidenciaanteriorinadimplencia") }}
    as dsprovidenciaanteriorinadimplencia,
    {{ bronze_inteiro("idusuarioefetivouindimplencia") }}
    as idusuarioefetivouindimplencia,
    {{ bronze_timestamp("dtadimplencia") }} as dtadimplencia,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_inteiro("iddocumentoanexado") }} as iddocumentoanexado,
    {{ bronze_inteiro("idusuarioliberouinadimplencia") }}
    as idusuarioliberouinadimplencia,
    {{ bronze_booleano("stposicionamento") }} as stposicionamento,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("salic", "bronze_sac_tbinadimplente") }}
