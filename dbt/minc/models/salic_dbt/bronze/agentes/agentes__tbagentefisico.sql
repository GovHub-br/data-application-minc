-- Bronze SALIC — agentes__tbagentefisico.
-- Origem: salic_bronze.agentes__tbagentefisico, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Uma linha por agente pessoa física, com atributos pessoais.
-- DADO SENSÍVEL: nome dos pais, cor/raça e necessidade especial. Não derive
-- nada disso sem base legal — ver as descrições no schema.yml.
select
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("stsexo") }} as stsexo,
    {{ bronze_texto("stestadocivil") }} as stestadocivil,
    {{ bronze_texto("stnecessidadeespecial") }} as stnecessidadeespecial,
    {{ bronze_texto("nmmae") }} as nmmae,
    {{ bronze_texto("nmpai") }} as nmpai,
    {{ bronze_timestamp("dtnascimento") }} as dtnascimento,
    {{ bronze_inteiro("stcorraca") }} as stcorraca,
    {{ bronze_texto("nridentificadorprocessual") }} as nridentificadorprocessual,
    _fatia
from {{ source("bronze_agentes", "agentes__tbagentefisico") }}
