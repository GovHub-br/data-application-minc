-- Bronze SALIC — bdcorporativo__tbencaminhamentoprestacaocontas.
-- Origem: salic_bronze.bdcorporativo__tbencaminhamentoprestacaocontas (schema
-- scsac do banco corporativo), tudo em texto da ingestão via Trino (ADR 0005).
-- Uma linha por encaminhamento de prestação de contas de um projeto entre
-- órgãos/agentes. `idsituacao` é código textual (E27); `idsituacaoencprestcontas`
-- aponta para bdcorporativo__tbsituacaoencaminhamentoprestacaocontas.
select
    {{ bronze_inteiro("idencprestcontas") }} as idencprestcontas,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idagenteorigem") }} as idagenteorigem,
    {{ bronze_timestamp("dtinicioencaminhamento") }} as dtinicioencaminhamento,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_inteiro("idorgaodestino") }} as idorgaodestino,
    {{ bronze_inteiro("idorgaoorigem") }} as idorgaoorigem,
    {{ bronze_inteiro("idagentedestino") }} as idagentedestino,
    {{ bronze_inteiro("cdgruposdestino") }} as cdgruposdestino,
    {{ bronze_inteiro("cdgruposorigem") }} as cdgruposorigem,
    {{ bronze_timestamp("dtfimencaminhamento") }} as dtfimencaminhamento,
    {{ bronze_inteiro("idsituacaoencprestcontas") }} as idsituacaoencprestcontas,
    {{ bronze_texto("idsituacao") }} as idsituacao,
    {{ bronze_booleano("stativo") }} as stativo,
    _fatia
from {{ source("bronze_bdcorporativo", "bdcorporativo__tbencaminhamentoprestacaocontas") }}
