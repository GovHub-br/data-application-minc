-- Silver SALIC — silver_agentes_agentes (ex-agentes__agentes).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_agentes, onde tudo chega como texto da ingestão
-- via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Uma linha por agente cultural (proponente, incentivador ou fornecedor).
-- `cnpjcpf` fica TEXT (documento com zero à esquerda, dado pessoal). `tipopessoa`
-- é bit da origem: false é pessoa física (CPF), true é jurídica (CNPJ).
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
from {{ source("salic", "bronze_agentes_agentes") }}
