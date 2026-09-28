-- Bronze SALIC — sac__tbpareceristaprorrogarprazo.
-- Origem: salic_bronze.sac__tbpareceristaprorrogarprazo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpareceristaprorrogarprazo") }} as idpareceristaprorrogarprazo,
    {{ bronze_inteiro("iddistribuir") }} as iddistribuir,
    {{ bronze_inteiro("tpanalise") }} as tpanalise,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_timestamp("dtconcessao") }} as dtconcessao,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__tbpareceristaprorrogarprazo") }}
