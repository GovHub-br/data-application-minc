-- Bronze SALIC — sac__tbcomprovantetrimestral.
-- Origem: salic_bronze.sac__tbcomprovantetrimestral, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 7 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcomprovantetrimestral") }} as idcomprovantetrimestral,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dtcomprovante") }} as dtcomprovante,
    {{ bronze_timestamp("dtinicioperiodo") }} as dtinicioperiodo,
    {{ bronze_timestamp("dtfimperiodo") }} as dtfimperiodo,
    {{ bronze_texto("dsetapasexecutadas") }} as dsetapasexecutadas,
    {{ bronze_texto("dsacessibilidade") }} as dsacessibilidade,
    {{ bronze_texto("dsdemocratizacaoacesso") }} as dsdemocratizacaoacesso,
    {{ bronze_texto("dsimpactoambiental") }} as dsimpactoambiental,
    {{ bronze_texto("sicomprovantetrimestral") }} as sicomprovantetrimestral,
    {{ bronze_inteiro("nrcomprovantetrimestral") }} as nrcomprovantetrimestral,
    {{ bronze_texto("idcadastrador") }} as idcadastrador,
    {{ bronze_texto("dsparecertecnico") }} as dsparecertecnico,
    {{ bronze_texto("dsrecomendacao") }} as dsrecomendacao,
    {{ bronze_inteiro("idtecnicoavaliador") }} as idtecnicoavaliador,
    _fatia
from {{ source("bronze_sac", "sac__tbcomprovantetrimestral") }}
