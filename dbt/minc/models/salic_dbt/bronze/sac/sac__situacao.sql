-- Bronze SALIC — sac__situacao.
-- Origem: salic_bronze.sac__situacao, onde tudo chega como texto da ingestão
-- via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Tabela de domínio: o código da situação (A01, K00, ...) e sua descrição. É a
-- ponta de `sac__projetos.situacao`. `codigo` fica TEXT porque é um código, não
-- um número.
select
    {{ bronze_texto("codigo") }} as codigo,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("areaatuacao") }} as areaatuacao,
    {{ bronze_booleano("statusprojeto") }} as statusprojeto,
    _fatia
from {{ source("bronze_sac", "sac__situacao") }}
