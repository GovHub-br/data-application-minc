-- Silver SALIC — silver_tabelas_v_logradouro_completo (ex-tabelas__v_logradouro_completo).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_v_logradouro_completo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 6 tipadas, 13 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("log_codigo") }} as log_codigo,
    {{ bronze_inteiro("log_tipo") }} as log_tipo,
    {{ bronze_texto("log_nome") }} as log_nome,
    {{ bronze_inteiro("log_localidade") }} as log_localidade,
    {{ bronze_texto("log_limites") }} as log_limites,
    {{ bronze_inteiro("log_bairro") }} as log_bairro,
    {{ bronze_inteiro("log_bairro_final") }} as log_bairro_final,
    {{ bronze_texto("log_cep") }} as log_cep,
    {{ bronze_texto("loc_estruturado") }} as loc_estruturado,
    {{ bronze_inteiro("loc_tipo") }} as loc_tipo,
    {{ bronze_texto("loc_sigla") }} as loc_sigla,
    {{ bronze_texto("loc_cep") }} as loc_cep,
    {{ bronze_texto("loc_pais") }} as loc_pais,
    {{ bronze_texto("loc_estado") }} as loc_estado,
    {{ bronze_texto("loc_municipio") }} as loc_municipio,
    {{ bronze_texto("loc_distrito") }} as loc_distrito,
    {{ bronze_texto("tlg_descricao") }} as tlg_descricao,
    {{ bronze_texto("bai_bairro") }} as bai_bairro,
    {{ bronze_texto("bai_bairro_final") }} as bai_bairro_final,
    _fatia
from {{ source("salic", "bronze_tabelas_v_logradouro_completo") }}
