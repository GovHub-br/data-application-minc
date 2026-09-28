-- Bronze SALIC — sac__tbcomplianceitemcomprovado.
-- Origem: salic_bronze.sac__tbcomplianceitemcomprovado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 14 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcompliancefinanceiro") }} as idcompliancefinanceiro,
    {{ bronze_inteiro("idcompliancecomprovacao") }} as idcompliancecomprovacao,
    {{ bronze_inteiro("idtipo") }} as idtipo,
    {{ bronze_inteiro("nrfonterecurso") }} as nrfonterecurso,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_inteiro("idmunicipio") }} as idmunicipio,
    {{ bronze_inteiro("idetapa") }} as idetapa,
    {{ bronze_inteiro("iditem") }} as iditem,
    {{ bronze_inteiro("idfornecedor") }} as idfornecedor,
    {{ bronze_texto("nrcomprovante") }} as nrcomprovante,
    {{ bronze_texto("nrlancamento") }} as nrlancamento,
    {{ bronze_numerico("vlcomprovado") }} as vlcomprovado,
    {{ bronze_numerico("vllancamento") }} as vllancamento,
    {{ bronze_numerico("vldiferenca") }} as vldiferenca,
    {{ bronze_texto("dsobservacao") }} as dsobservacao,
    {{ bronze_inteiro("idcomprovantepagamento") }} as idcomprovantepagamento,
    _fatia
from {{ source("bronze_sac", "sac__tbcomplianceitemcomprovado") }}
