-- Bronze SALIC — bdcorporativo__tbsituacaoencaminhamentoprestacaocontas.
-- Origem: salic_bronze.bdcorporativo__tbsituacaoencaminhamentoprestacaocontas
-- (schema scsac do banco corporativo), tudo em texto da ingestão via Trino
-- (ADR 0005). Tabela de domínio: as situações possíveis de um encaminhamento de
-- prestação de contas. É a ponta de
-- bdcorporativo__tbencaminhamentoprestacaocontas.idsituacaoencprestcontas.
select
    {{ bronze_inteiro("idsituacaoencprestcontas") }} as idsituacaoencprestcontas,
    {{ bronze_texto("dssituacaoencprestcontas") }} as dssituacaoencprestcontas,
    _fatia
from {{ source("bronze_bdcorporativo", "bdcorporativo__tbsituacaoencaminhamentoprestacaocontas") }}
