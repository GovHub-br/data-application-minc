-- Bronze SALIC — sac__tbprojetoreadequacao.
-- Origem: salic_bronze.sac__tbprojetoreadequacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 4 tipadas, 13 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojetoreadequacao") }} as idprojetoreadequacao,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("dsresumodoprojeto") }} as dsresumodoprojeto,
    {{ bronze_texto("dsobjetivos") }} as dsobjetivos,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_texto("dsacessibilidade") }} as dsacessibilidade,
    {{ bronze_texto("dsdemocratizacaodeacesso") }} as dsdemocratizacaodeacesso,
    {{ bronze_texto("dsetapadetrabalho") }} as dsetapadetrabalho,
    {{ bronze_texto("dsfichatecnica") }} as dsfichatecnica,
    {{ bronze_texto("dssinopse") }} as dssinopse,
    {{ bronze_texto("dsimpactoambiental") }} as dsimpactoambiental,
    {{ bronze_texto("dsespecificacaotecnica") }} as dsespecificacaotecnica,
    {{ bronze_texto("dsestrategiadeexecucao") }} as dsestrategiadeexecucao,
    {{ bronze_texto("dsdescricaoatividade") }} as dsdescricaoatividade,
    _fatia
from {{ source("bronze_sac", "sac__tbprojetoreadequacao") }}
