-- Bronze SALIC — sac__vwpautados_cnic_mais_trinta_dias.
-- Origem: salic_bronze.sac__vwpautados_cnic_mais_trinta_dias, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 1 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("conselheiro") }} as conselheiro,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_inteiro("qtdias") }} as qtdias,
    {{ bronze_texto("tipo") }} as tipo,
    {{ bronze_texto("recurso") }} as recurso,
    _fatia
from {{ source("bronze_sac", "sac__vwpautados_cnic_mais_trinta_dias") }}
