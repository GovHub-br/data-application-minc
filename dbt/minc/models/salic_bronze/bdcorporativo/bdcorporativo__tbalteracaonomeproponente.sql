-- Bronze SALIC — bdcorporativo__tbalteracaonomeproponente.
-- Origem: salic_bronze.bdcorporativo__tbalteracaonomeproponente (schema scsac do
-- banco corporativo), tudo em texto da ingestão via Trino (ADR 0005).
-- Pedidos de alteração do nome do proponente. `nrcnpjcpf` fica TEXT (documento,
-- zero à esquerda, dado pessoal).
select
    {{ bronze_inteiro("idpedidoalteracao") }} as idpedidoalteracao,
    {{ bronze_texto("nrcnpjcpf") }} as nrcnpjcpf,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    _fatia
from {{ source("bronze_bdcorporativo", "bdcorporativo__tbalteracaonomeproponente") }}
