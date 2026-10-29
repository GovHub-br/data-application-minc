-- Silver SALIC — silver_bdcorporativo_tbsituacaoencaminhamentoprestacaocontas
-- (ex-bdcorporativo__tbsituacaoencaminhamentoprestacaocontas).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_bdcorporativo_tbsituacaoencaminhamentoprestacaocontas
-- (schema scsac do banco corporativo), tudo em texto da ingestão via Trino
-- (ADR 0005). Tabela de domínio: as situações possíveis de um encaminhamento de
-- prestação de contas. É a ponta de
-- bdcorporativo__tbencaminhamentoprestacaocontas.idsituacaoencprestcontas.
select
    {{ bronze_inteiro("idsituacaoencprestcontas") }} as idsituacaoencprestcontas,
    {{ bronze_texto("dssituacaoencprestcontas") }} as dssituacaoencprestcontas,
    _fatia
from {{ source("salic", "bronze_bdcorporativo_tbsituacaoencaminhamentoprestacaocontas") }}
