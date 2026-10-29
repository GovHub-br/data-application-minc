-- Silver SALIC — silver_sac_tbcustosvinculados (ex-sac__tbcustosvinculados).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbcustosvinculados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcustosvinculados") }} as idcustosvinculados,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_inteiro("idplanilhaitem") }} as idplanilhaitem,
    {{ bronze_timestamp("dtcadastro") }} as dtcadastro,
    {{ bronze_texto("dsobservacao") }} as dsobservacao,
    {{ bronze_numerico("pccalculo") }} as pccalculo,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_tbcustosvinculados") }}
