-- Silver SALIC — silver_sac_tbhistoricoemail (ex-sac__tbhistoricoemail).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbhistoricoemail, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 8 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhistoricoemail") }} as idhistoricoemail,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idavaliacaoproposta") }} as idavaliacaoproposta,
    {{ bronze_inteiro("idtextoemail") }} as idtextoemail,
    {{ bronze_timestamp("dtemail") }} as dtemail,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_tbhistoricoemail") }}
