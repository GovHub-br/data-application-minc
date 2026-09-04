-- Bronze SALIC — sac__tbapicomprovacoes.
-- Origem: salic_bronze.sac__tbapicomprovacoes, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Uma linha por comprovante de pagamento da execução do projeto, publicado pela
-- API do SALIC. É a maior das tabelas deste lote (~2,6 M linhas).
-- ATENÇÃO: `sguf` não contém sigla de UF — ver a descrição no schema.yml.
select
    {{ bronze_inteiro("idapicomprovacoes") }} as idapicomprovacoes,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nrpronac") }} as nrpronac,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("nmitem") }} as nmitem,
    {{ bronze_texto("nmfornecedor") }} as nmfornecedor,
    {{ bronze_texto("tpfornecedor") }} as tpfornecedor,
    {{ bronze_texto("dsregiao") }} as dsregiao,
    {{ bronze_texto("sguf") }} as sguf,
    {{ bronze_texto("nmmunicipio") }} as nmmunicipio,
    {{ bronze_data("dtcomprovacao") }} as dtcomprovacao,
    {{ bronze_inteiro("aaanocomprovacao") }} as aaanocomprovacao,
    {{ bronze_inteiro("mmmescomprovacao") }} as mmmescomprovacao,
    {{ bronze_texto("tpdocumento") }} as tpdocumento,
    {{ bronze_texto("nrcomprovante") }} as nrcomprovante,
    {{ bronze_data("dtemissao") }} as dtemissao,
    {{ bronze_texto("tpformadepagamento") }} as tpformadepagamento,
    {{ bronze_texto("nrdocumentodepagamento") }} as nrdocumentodepagamento,
    {{ bronze_numerico("vlpagamento") }} as vlpagamento,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_texto("nmarquivo") }} as nmarquivo,
    {{ bronze_texto("hashregistro") }} as hashregistro,
    {{ bronze_timestamp("dtatualizacao") }} as dtatualizacao,
    _fatia
from {{ source("bronze_sac", "sac__tbapicomprovacoes") }}
