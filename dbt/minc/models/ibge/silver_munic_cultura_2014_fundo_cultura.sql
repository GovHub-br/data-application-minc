-- Silver ibge — munic_cultura_2014_fundo_cultura.
-- Origem: bronze_munic_cultura_2014_fundo_cultura, cópia fiel de
-- seeds.ibge_munic_cultura_2014_fundo_cultura, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 35 colunas: 1 tipadas, 34 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("a1", "bigint") }} as a1,
    {{ bronze_texto("a372") }} as a372,
    {{ bronze_texto("a373") }} as a373,
    {{ bronze_texto("a374") }} as a374,
    {{ bronze_texto("a375") }} as a375,
    {{ bronze_texto("a376") }} as a376,
    {{ bronze_texto("a377") }} as a377,
    {{ bronze_texto("a378") }} as a378,
    {{ bronze_texto("a379") }} as a379,
    {{ bronze_texto("a380") }} as a380,
    {{ bronze_texto("a381") }} as a381,
    {{ bronze_texto("a382") }} as a382,
    {{ bronze_texto("a383") }} as a383,
    {{ bronze_texto("a384") }} as a384,
    {{ bronze_texto("a385") }} as a385,
    {{ bronze_texto("a386") }} as a386,
    {{ bronze_texto("a387") }} as a387,
    {{ bronze_texto("a388") }} as a388,
    {{ bronze_texto("a389") }} as a389,
    {{ bronze_texto("a390") }} as a390,
    {{ bronze_texto("a391") }} as a391,
    {{ bronze_texto("a392") }} as a392,
    {{ bronze_texto("a393") }} as a393,
    {{ bronze_texto("a394") }} as a394,
    {{ bronze_texto("a395") }} as a395,
    {{ bronze_texto("a396") }} as a396,
    {{ bronze_texto("a397") }} as a397,
    {{ bronze_texto("a398") }} as a398,
    {{ bronze_texto("a399") }} as a399,
    {{ bronze_texto("a400") }} as a400,
    {{ bronze_texto("a401") }} as a401,
    {{ bronze_texto("a402") }} as a402,
    {{ bronze_texto("a403") }} as a403,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_timestamptz("dt_ingest") }} as dt_ingest
from {{ ref("bronze_munic_cultura_2014_fundo_cultura") }}
