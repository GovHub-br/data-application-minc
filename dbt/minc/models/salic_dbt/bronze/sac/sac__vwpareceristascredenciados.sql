-- Bronze SALIC — sac__vwpareceristascredenciados.
-- Origem: salic_bronze.sac__vwpareceristascredenciados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 2 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("nrcnpjcpf") }} as nrcnpjcpf,
    {{ bronze_texto("nmparecerista") }} as nmparecerista,
    {{ bronze_inteiro("cdarea") }} as cdarea,
    {{ bronze_texto("nmarea") }} as nmarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_texto("nmsegmento") }} as nmsegmento,
    {{ bronze_texto("cdorgaosuperior") }} as cdorgaosuperior,
    {{ bronze_texto("sgorgaoautorizado") }} as sgorgaoautorizado,
    {{ bronze_texto("nrtelefone") }} as nrtelefone,
    {{ bronze_texto("eecorreioeletronico") }} as eecorreioeletronico,
    _fatia
from {{ source("bronze_sac", "sac__vwpareceristascredenciados") }}
