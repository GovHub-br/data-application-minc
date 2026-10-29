-- Silver SALIC — silver_bdcorporativo_tbalteracaonomeproponente
-- (ex-bdcorporativo__tbalteracaonomeproponente).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_bdcorporativo_tbalteracaonomeproponente (schema scsac do
-- banco corporativo), tudo em texto da ingestão via Trino (ADR 0005).
-- Pedidos de alteração do nome do proponente. `nrcnpjcpf` fica TEXT (documento,
-- zero à esquerda, dado pessoal).
select
    {{ bronze_inteiro("idpedidoalteracao") }} as idpedidoalteracao,
    {{ bronze_texto("nrcnpjcpf") }} as nrcnpjcpf,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    _fatia
from {{ source("salic", "bronze_bdcorporativo_tbalteracaonomeproponente") }}
