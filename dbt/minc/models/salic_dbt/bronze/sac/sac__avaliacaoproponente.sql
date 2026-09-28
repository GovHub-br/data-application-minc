-- Bronze SALIC — sac__avaliacaoproponente.
-- Origem: salic_bronze.sac__avaliacaoproponente, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("codparametro") }} as codparametro,
    {{ bronze_inteiro("codpeso") }} as codpeso,
    _fatia
from {{ source("bronze_sac", "sac__avaliacaoproponente") }}
