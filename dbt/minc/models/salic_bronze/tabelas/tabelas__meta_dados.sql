-- Bronze SALIC — tabelas__meta_dados.
-- Origem: salic_bronze.tabelas__meta_dados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 7 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("met_tabela") }} as met_tabela,
    {{ bronze_inteiro("met_codigo") }} as met_codigo,
    {{ bronze_texto("met_nome") }} as met_nome,
    {{ bronze_inteiro("met_ocorrencia") }} as met_ocorrencia,
    {{ bronze_inteiro("met_tamanho") }} as met_tamanho,
    {{ bronze_inteiro("met_tipo") }} as met_tipo,
    {{ bronze_texto("met_criterio") }} as met_criterio,
    {{ bronze_inteiro("met_indice_palavras") }} as met_indice_palavras,
    {{ bronze_inteiro("met_status") }} as met_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__meta_dados") }}
