-- Silver ibge — munic_cultura_2006_equipamentos.
-- Origem: bronze_munic_cultura_2006_equipamentos, cópia fiel de
-- seeds.ibge_munic_cultura_2006_equipamentos, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 27 colunas: 1 tipadas, 26 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("a1", "bigint") }} as a1,
    {{ bronze_texto("a424") }} as a424,
    {{ bronze_texto("a425") }} as a425,
    {{ bronze_texto("a426") }} as a426,
    {{ bronze_texto("a427") }} as a427,
    {{ bronze_texto("a428") }} as a428,
    {{ bronze_texto("a429") }} as a429,
    {{ bronze_texto("a430") }} as a430,
    {{ bronze_texto("a431") }} as a431,
    {{ bronze_texto("a432") }} as a432,
    {{ bronze_texto("a433") }} as a433,
    {{ bronze_texto("a434") }} as a434,
    {{ bronze_texto("a435") }} as a435,
    {{ bronze_texto("a436") }} as a436,
    {{ bronze_texto("a437") }} as a437,
    {{ bronze_texto("a438") }} as a438,
    {{ bronze_texto("a439") }} as a439,
    {{ bronze_texto("a440") }} as a440,
    {{ bronze_texto("a441") }} as a441,
    {{ bronze_texto("a442") }} as a442,
    {{ bronze_texto("a443") }} as a443,
    {{ bronze_texto("a444") }} as a444,
    {{ bronze_texto("a285") }} as a285,
    {{ bronze_texto("a445") }} as a445,
    {{ bronze_texto("a446") }} as a446,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_texto("dt_ingest") }} as dt_ingest
from {{ ref("bronze_munic_cultura_2006_equipamentos") }}
