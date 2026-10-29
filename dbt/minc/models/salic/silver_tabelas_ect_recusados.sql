-- Silver SALIC — silver_tabelas_ect_recusados (ex-tabelas__ect_recusados).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_ect_recusados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 0 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("ectr_tipo") }} as ectr_tipo,
    {{ bronze_texto("ectr_uf") }} as ectr_uf,
    {{ bronze_texto("ectr_cep5") }} as ectr_cep5,
    {{ bronze_texto("ectr_cep8") }} as ectr_cep8,
    {{ bronze_texto("ectr_local") }} as ectr_local,
    {{ bronze_texto("ectr_tipo_local") }} as ectr_tipo_local,
    {{ bronze_texto("ectr_nome") }} as ectr_nome,
    {{ bronze_texto("ectr_bairro") }} as ectr_bairro,
    {{ bronze_texto("ectr_bairro_final") }} as ectr_bairro_final,
    {{ bronze_texto("ectr_tipo_lograd") }} as ectr_tipo_lograd,
    _fatia
from {{ source("salic", "bronze_tabelas_ect_recusados") }}
