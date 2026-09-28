-- Bronze SALIC — agentes__agentes.
-- Origem: salic_bronze.agentes__agentes, onde tudo chega como texto da ingestão
-- via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Uma linha por agente cultural (proponente, incentivador ou fornecedor).
-- `cnpjcpf` fica TEXT (documento com zero à esquerda, dado pessoal). `tipopessoa`
-- é bit da origem — ver a ressalva de semântica no schema.yml.
select
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("cnpjcpfsuperior") }} as cnpjcpfsuperior,
    {{ bronze_booleano("tipopessoa") }} as tipopessoa,
    {{ bronze_timestamp("dtcadastro") }} as dtcadastro,
    {{ bronze_timestamp("dtatualizacao") }} as dtatualizacao,
    {{ bronze_timestamp("dtvalidade") }} as dtvalidade,
    {{ bronze_booleano("status") }} as status,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("bronze_agentes", "agentes__agentes") }}
