-- Bronze SALIC — tabelas__pessoas.
-- Origem: salic_bronze.tabelas__pessoas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 14 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pes_codigo") }} as pes_codigo,
    {{ bronze_inteiro("pes_categoria") }} as pes_categoria,
    {{ bronze_inteiro("pes_tipo") }} as pes_tipo,
    {{ bronze_inteiro("pes_esfera") }} as pes_esfera,
    {{ bronze_inteiro("pes_administracao") }} as pes_administracao,
    {{ bronze_inteiro("pes_utilidade_publica") }} as pes_utilidade_publica,
    {{ bronze_inteiro("pes_superior") }} as pes_superior,
    {{ bronze_inteiro("pes_validade") }} as pes_validade,
    {{ bronze_inteiro("pes_orgao_cadastrador") }} as pes_orgao_cadastrador,
    {{ bronze_inteiro("pes_usuario_cadastrador") }} as pes_usuario_cadastrador,
    {{ bronze_timestamp("pes_data_cadastramento") }} as pes_data_cadastramento,
    {{ bronze_inteiro("pes_orgao_atualizador") }} as pes_orgao_atualizador,
    {{ bronze_inteiro("pes_usuario_atualizador") }} as pes_usuario_atualizador,
    {{ bronze_timestamp("pes_data_atualizacao") }} as pes_data_atualizacao,
    {{ bronze_texto("pes_controle") }} as pes_controle,
    _fatia
from {{ source("bronze_tabelas", "tabelas__pessoas") }}
