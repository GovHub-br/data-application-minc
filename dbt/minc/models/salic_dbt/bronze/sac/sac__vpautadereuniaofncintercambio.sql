-- Bronze SALIC — sac__vpautadereuniaofncintercambio.
-- Origem: salic_bronze.sac__vpautadereuniaofncintercambio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 0 tipadas, 19 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("evento") }} as evento,
    {{ bronze_texto("entidadepromotora") }} as entidadepromotora,
    {{ bronze_texto("cidadeevento") }} as cidadeevento,
    {{ bronze_texto("pais") }} as pais,
    {{ bronze_texto("processo") }} as processo,
    {{ bronze_texto("resumoprojeto") }} as resumoprojeto,
    {{ bronze_texto("parecertecnico") }} as parecertecnico,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("resumocurriculo") }} as resumocurriculo,
    {{ bronze_texto("vlnormal") }} as vlnormal,
    {{ bronze_texto("vlpromocional") }} as vlpromocional,
    {{ bronze_texto("vlnormaltotal") }} as vlnormaltotal,
    {{ bronze_texto("vlpromocionaltotal") }} as vlpromocionaltotal,
    {{ bronze_texto("dtinicio") }} as dtinicio,
    _fatia
from {{ source("bronze_sac", "sac__vpautadereuniaofncintercambio") }}
