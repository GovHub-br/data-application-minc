-- Bronze SALIC — sac__tbcontacartao.
-- Origem: salic_bronze.sac__tbcontacartao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 4 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcontacartao") }} as idcontacartao,
    {{ bronze_inteiro("idcontabancaria") }} as idcontabancaria,
    {{ bronze_texto("nrcontacartao") }} as nrcontacartao,
    {{ bronze_texto("nrcentrodecusto") }} as nrcentrodecusto,
    {{ bronze_texto("nrcpfportadordocartao") }} as nrcpfportadordocartao,
    {{ bronze_texto("cdproponentenobanco") }} as cdproponentenobanco,
    {{ bronze_texto("cdportadorcartaonobanco") }} as cdportadorcartaonobanco,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbcontacartao") }}
