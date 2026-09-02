-- Bronze SALIC — sac__bandas.
-- Origem: salic_bronze.sac__bandas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 11 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("finalidade") }} as finalidade,
    {{ bronze_timestamp("dtfundacao") }} as dtfundacao,
    {{ bronze_inteiro("numerocomponentes") }} as numerocomponentes,
    {{ bronze_texto("regente") }} as regente,
    {{ bronze_booleano("indicacao") }} as indicacao,
    {{ bronze_booleano("emenda") }} as emenda,
    {{ bronze_booleano("pmunicipal") }} as pmunicipal,
    {{ bronze_booleano("pestadual") }} as pestadual,
    {{ bronze_booleano("pregional") }} as pregional,
    {{ bronze_booleano("pnacional") }} as pnacional,
    {{ bronze_booleano("pinternacional") }} as pinternacional,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__bandas") }}
