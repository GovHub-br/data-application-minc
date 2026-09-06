-- Bronze SALIC — sac__distribuicao_avaliacao_proposta_bkp.
-- Origem: salic_bronze.sac__distribuicao_avaliacao_proposta_bkp, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 6 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("id_distribuicao_avaliacao_proposta") }}
    as id_distribuicao_avaliacao_proposta,
    {{ bronze_inteiro("id_preprojeto") }} as id_preprojeto,
    {{ bronze_inteiro("id_orgao_superior") }} as id_orgao_superior,
    {{ bronze_inteiro("id_perfil") }} as id_perfil,
    {{ bronze_timestamp("data_distribuicao") }} as data_distribuicao,
    {{ bronze_booleano("avaliacao_atual") }} as avaliacao_atual,
    _fatia
from {{ source("bronze_sac", "sac__distribuicao_avaliacao_proposta_bkp") }}
