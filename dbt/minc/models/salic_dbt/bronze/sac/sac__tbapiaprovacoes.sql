-- Bronze SALIC — sac__tbapiaprovacoes.
-- Origem: salic_bronze.sac__tbapiaprovacoes, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Uma linha por aprovação/prorrogação do projeto, publicada pela API do SALIC,
-- com a portaria, as datas de captação e o valor aprovado. `hashregistro` é hash
-- SHA em hex; `dtatualizacao` é o instante da carga da API.
select
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_inteiro("idaprovacao") }} as idaprovacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nrpronac") }} as nrpronac,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_texto("tpaprovacao") }} as tpaprovacao,
    {{ bronze_texto("resumoaprovacao") }} as resumoaprovacao,
    {{ bronze_texto("portariaaprovacao") }} as portariaaprovacao,
    {{ bronze_data("dtportariaaprovacao") }} as dtportariaaprovacao,
    {{ bronze_data("dtpublicacaoaprovacao") }} as dtpublicacaoaprovacao,
    {{ bronze_data("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_data("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_texto("hashregistro") }} as hashregistro,
    {{ bronze_timestamp("dtatualizacao") }} as dtatualizacao,
    _fatia
from {{ source("bronze_sac", "sac__tbapiaprovacoes") }}
