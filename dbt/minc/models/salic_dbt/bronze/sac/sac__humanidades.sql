-- Bronze SALIC — sac__humanidades.
-- Origem: salic_bronze.sac__humanidades, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 12 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("opcao") }} as opcao,
    {{ bronze_inteiro("tiragem") }} as tiragem,
    {{ bronze_inteiro("edicao") }} as edicao,
    {{ bronze_inteiro("periocidade") }} as periocidade,
    {{ bronze_inteiro("nredicao") }} as nredicao,
    {{ bronze_inteiro("acervo") }} as acervo,
    {{ bronze_inteiro("equipamento") }} as equipamento,
    {{ bronze_inteiro("pdpatrocinio") }} as pdpatrocinio,
    {{ bronze_inteiro("pddoacao") }} as pddoacao,
    {{ bronze_inteiro("pdcomercializacao") }} as pdcomercializacao,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idhumanidades") }} as idhumanidades,
    _fatia
from {{ source("bronze_sac", "sac__humanidades") }}
