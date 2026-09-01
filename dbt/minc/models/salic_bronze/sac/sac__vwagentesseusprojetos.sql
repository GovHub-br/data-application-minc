-- Bronze SALIC — sac__vwagentesseusprojetos.
-- Origem: salic_bronze.sac__vwagentesseusprojetos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 6 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("ordem") }} as ordem,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dtiniciodeexecucao") }} as dtiniciodeexecucao,
    {{ bronze_timestamp("dtfinaldeexecucao") }} as dtfinaldeexecucao,
    {{ bronze_inteiro("mecanismo") }} as mecanismo,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("nomeproponente") }} as nomeproponente,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("idsolicitante") }} as idsolicitante,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__vwagentesseusprojetos") }}
