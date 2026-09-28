-- Bronze SALIC — sac__tbdistribuirparecer.
-- Origem: salic_bronze.sac__tbdistribuirparecer, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 17 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("tipoanalise") }} as tipoanalise,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_inteiro("idagenteparecerista") }} as idagenteparecerista,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_timestamp("dtdevolucao") }} as dtdevolucao,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_booleano("stprincipal") }} as stprincipal,
    {{ bronze_texto("fecharanalise") }} as fecharanalise,
    {{ bronze_timestamp("dtretorno") }} as dtretorno,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_booleano("stdiligenciado") }} as stdiligenciado,
    {{ bronze_inteiro("siencaminhamento") }} as siencaminhamento,
    {{ bronze_inteiro("sianalise") }} as sianalise,
    {{ bronze_inteiro("idorgaoorigem") }} as idorgaoorigem,
    _fatia
from {{ source("bronze_sac", "sac__tbdistribuirparecer") }}
