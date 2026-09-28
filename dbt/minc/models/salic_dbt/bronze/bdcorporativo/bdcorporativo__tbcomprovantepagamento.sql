-- Bronze SALIC — bdcorporativo__tbcomprovantepagamento.
-- Origem: salic_bronze.bdcorporativo__tbcomprovantepagamento (schema scsac do
-- banco corporativo), tudo em texto da ingestão via Trino (ADR 0005).
-- Uma linha por comprovante de pagamento da execução (~4,2 M linhas). Números de
-- comprovante/documento ficam TEXT (identificadores); valores e datas convertidos.
select
    {{ bronze_inteiro("tpdocumento") }} as tpdocumento,
    {{ bronze_texto("nrcomprovante") }} as nrcomprovante,
    {{ bronze_texto("nrserie") }} as nrserie,
    {{ bronze_inteiro("idcomprovantepagamento") }} as idcomprovantepagamento,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_numerico("vlcomprovacao") }} as vlcomprovacao,
    {{ bronze_timestamp("dtemissao") }} as dtemissao,
    {{ bronze_timestamp("dtpagamento") }} as dtpagamento,
    {{ bronze_inteiro("idfornecedor") }} as idfornecedor,
    {{ bronze_texto("idfornecedorexterior") }} as idfornecedorexterior,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_texto("dsoutrasfontes") }} as dsoutrasfontes,
    {{ bronze_inteiro("tpformadepagamento") }} as tpformadepagamento,
    {{ bronze_texto("nrdocumentodepagamento") }} as nrdocumentodepagamento,
    {{ bronze_texto("tbcomprovantepagamento") }} as tbcomprovantepagamento,
    _fatia
from {{ source("bronze_bdcorporativo", "bdcorporativo__tbcomprovantepagamento") }}
