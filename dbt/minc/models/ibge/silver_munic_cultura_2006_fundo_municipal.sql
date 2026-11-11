-- Silver ibge — munic_cultura_2006_fundo_municipal.
-- Origem: bronze_munic_cultura_2006_fundo_municipal, cópia fiel de
-- seeds.ibge_munic_cultura_2006_fundo_municipal, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 36 colunas: 1 tipadas, 35 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("a1", "bigint") }} as a1,
    {{ bronze_texto("a196") }} as a196,
    {{ bronze_texto("a197") }} as a197,
    {{ bronze_texto("a198") }} as a198,
    {{ bronze_texto("a199") }} as a199,
    {{ bronze_texto("a200") }} as a200,
    {{ bronze_texto("a201") }} as a201,
    {{ bronze_texto("a202") }} as a202,
    {{ bronze_texto("a203") }} as a203,
    {{ bronze_texto("a204") }} as a204,
    {{ bronze_texto("a205") }} as a205,
    {{ bronze_texto("a206") }} as a206,
    {{ bronze_texto("a207") }} as a207,
    {{ bronze_texto("a208") }} as a208,
    {{ bronze_texto("a209") }} as a209,
    {{ bronze_texto("a210") }} as a210,
    {{ bronze_texto("a211") }} as a211,
    {{ bronze_texto("a212") }} as a212,
    {{ bronze_texto("a213") }} as a213,
    {{ bronze_texto("a214") }} as a214,
    {{ bronze_texto("a215") }} as a215,
    {{ bronze_texto("a216") }} as a216,
    {{ bronze_texto("a217") }} as a217,
    {{ bronze_texto("a218") }} as a218,
    {{ bronze_texto("a219") }} as a219,
    {{ bronze_texto("a220") }} as a220,
    {{ bronze_texto("a221") }} as a221,
    {{ bronze_texto("a222") }} as a222,
    {{ bronze_texto("a223") }} as a223,
    {{ bronze_texto("a224") }} as a224,
    {{ bronze_texto("a225") }} as a225,
    {{ bronze_texto("a226") }} as a226,
    {{ bronze_texto("a227") }} as a227,
    {{ bronze_texto("a228") }} as a228,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_timestamptz("dt_ingest") }} as dt_ingest
from {{ ref("bronze_munic_cultura_2006_fundo_municipal") }}
