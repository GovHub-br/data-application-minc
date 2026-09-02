-- Bronze SALIC — sac__vwpainelcoordenadorvinculadasreanalisar.
-- Origem: salic_bronze.sac__vwpainelcoordenadorvinculadasreanalisar, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 23 colunas: 0 tipadas, 22 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("idpronac") }} as idpronac,
    {{ bronze_texto("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("idproduto") }} as idproduto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("idarea") }} as idarea,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("idsegmento") }} as idsegmento,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_texto("idorgao") }} as idorgao,
    {{ bronze_texto("idagenteparecerista") }} as idagenteparecerista,
    {{ bronze_texto("parecerista") }} as parecerista,
    {{ bronze_texto("tipoanalise") }} as tipoanalise,
    {{ bronze_texto("dtenviomincvinculada") }} as dtenviomincvinculada,
    {{ bronze_texto("qtdiasdistribuir") }} as qtdiasdistribuir,
    {{ bronze_texto("justcomponente") }} as justcomponente,
    {{ bronze_texto("justdevolucaopedido") }} as justdevolucaopedido,
    {{ bronze_texto("justsecretaria") }} as justsecretaria,
    {{ bronze_texto("valor") }} as valor,
    {{ bronze_texto("stprincipal") }} as stprincipal,
    {{ bronze_texto("fecharanalise") }} as fecharanalise,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelcoordenadorvinculadasreanalisar") }}
