-- Bronze SALIC — sac__tbpareceristaprorrogarprazoxdistribuicao.
-- Origem: salic_bronze.sac__tbpareceristaprorrogarprazoxdistribuicao, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 5 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpareceristaprorrogarprazoxdistribuicao") }}
    as idpareceristaprorrogarprazoxdistribuicao,
    {{ bronze_inteiro("idpareceristaprorrogarprazo") }} as idpareceristaprorrogarprazo,
    {{ bronze_inteiro("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_inteiro("iddsitribuirreadequacao") }} as iddsitribuirreadequacao,
    {{ bronze_inteiro("iddistribuirrecurso") }} as iddistribuirrecurso,
    _fatia
from {{ source("bronze_sac", "sac__tbpareceristaprorrogarprazoxdistribuicao") }}
