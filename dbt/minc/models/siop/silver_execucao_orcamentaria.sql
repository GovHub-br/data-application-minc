-- Silver siop — execucao_orcamentaria.
-- Origem: bronze_execucao_orcamentaria, cópia fiel de
-- seeds.siop_execucao_orcamentaria, onde tudo chega como text da carga de
-- fonte estática (siop_execucao_dag, ADR 0012). Tipar é o trabalho desta
-- camada. Os códigos de órgão, UO, função e resultado primário ficam texto: a
-- UO tem zero à esquerda ('01101') e é assim que o SIOP a escreve.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("orgao_cod") }} as orgao_cod,
    {{ bronze_texto("orgao_nome") }} as orgao_nome,
    {{ bronze_texto("funcao_cod") }} as funcao_cod,
    {{ bronze_texto("funcao_nome") }} as funcao_nome,
    {{ bronze_texto("uo_cod") }} as uo_cod,
    {{ bronze_texto("uo_nome") }} as uo_nome,
    {{ bronze_texto("rp_cod") }} as rp_cod,
    {{ bronze_texto("rp_nome") }} as rp_nome,
    {{ bronze_numerico("projeto_lei") }} as projeto_lei,
    {{ bronze_numerico("dotacao_inicial") }} as dotacao_inicial,
    {{ bronze_numerico("dotacao_atual") }} as dotacao_atual,
    {{ bronze_numerico("empenhado") }} as empenhado,
    {{ bronze_numerico("liquidado") }} as liquidado,
    {{ bronze_numerico("pago") }} as pago,
    {{ bronze_texto("dt_ingest") }} as dt_ingest
from {{ ref("bronze_execucao_orcamentaria") }}
