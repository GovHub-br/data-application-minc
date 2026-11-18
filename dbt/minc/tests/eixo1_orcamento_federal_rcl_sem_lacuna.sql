-- Todo exercício do gasto federal anterior à var
-- eixo1_orcamento_federal_rcl_siconfi_desde tem a RCL do Tesouro. Sem este
-- teste, um ano que sumisse da carga do Tesouro deixaria a participação nula
-- em silêncio, porque as golds mantêm o exercício sem RCL. Quando o ramo do
-- SICONFI entrar na gold-base, o filtro de ano sai daqui.
select ano, valor_empenhado
from {{ ref("eixo1_orcamento_federal_impacto_rcl") }}
where rcl is null and ano < {{ var("eixo1_orcamento_federal_rcl_siconfi_desde", 2018) }}
