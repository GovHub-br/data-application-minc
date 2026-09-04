-- Bronze SALIC — sac__tbapicomprovacoes.
-- Origem: salic_bronze.sac__tbapicomprovacoes, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
--
-- O ano fica tipado como inteiro, e ano não numérico vira NULL. Isso é
-- deliberado: a série do SALIC tem marcador de ano desconhecido, e é melhor
-- que ele apareça como nulo do que como uma categoria falsa.
select
    {{ bronze_texto("idapicomprovacoes") }} as id_comprovacao,
    {{ bronze_inteiro("idpronac") }} as id_pronac,
    {{ bronze_texto("nrpronac") }} as pronac,
    {{ bronze_texto("nmprojeto") }} as nome_projeto,
    {{ bronze_texto("nmfornecedor") }} as nome_fornecedor,
    {{ bronze_texto("tpfornecedor") }} as tipo_fornecedor,
    {{ bronze_inteiro("aaanocomprovacao") }} as ano_comprovacao,
    {{ bronze_inteiro("mmmescomprovacao") }} as mes_comprovacao,
    {{ bronze_numerico("vlpagamento") }} as valor_pagamento,
    {{ bronze_texto("sguf") }} as sigla_uf,
    {{ bronze_texto("nmmunicipio") }} as nome_municipio,
    {{ bronze_texto("dsregiao") }} as regiao,
    _fatia
from {{ source("bronze_sac", "sac__tbapicomprovacoes") }}
