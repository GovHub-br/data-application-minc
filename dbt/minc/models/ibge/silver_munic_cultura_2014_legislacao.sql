-- Silver ibge — munic_cultura_2014_legislacao.
-- Origem: bronze_munic_cultura_2014_legislacao, cópia fiel de
-- seeds.ibge_munic_cultura_2014_legislacao, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 22 colunas: 1 tipadas, 21 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("a1", "bigint") }} as a1,
    {{ bronze_texto("a286") }} as a286,
    {{ bronze_texto("a287") }} as a287,
    {{ bronze_texto("a288") }} as a288,
    {{ bronze_texto("a289") }} as a289,
    {{ bronze_texto("a290") }} as a290,
    {{ bronze_texto("a291") }} as a291,
    {{ bronze_texto("a292") }} as a292,
    {{ bronze_texto("a293") }} as a293,
    {{ bronze_texto("a294") }} as a294,
    {{ bronze_texto("a295") }} as a295,
    {{ bronze_texto("a296") }} as a296,
    {{ bronze_texto("a297") }} as a297,
    {{ bronze_texto("a298") }} as a298,
    {{ bronze_texto("a299") }} as a299,
    {{ bronze_texto("a300") }} as a300,
    {{ bronze_texto("a301") }} as a301,
    {{ bronze_texto("a302") }} as a302,
    {{ bronze_texto("a303") }} as a303,
    {{ bronze_texto("a304") }} as a304,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_timestamptz("dt_ingest") }} as dt_ingest
from {{ ref("bronze_munic_cultura_2014_legislacao") }}
