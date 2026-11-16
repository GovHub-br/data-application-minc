-- Gold-base do Eixo 1, frente orçamento, esfera federal: a Receita Corrente
-- Líquida da União por ano, em reais, com a fonte de cada ano. É o denominador
-- das golds federais que medem o gasto em cultura como fração da RCL; elas
-- leem daqui, e não das silvers, para que a escolha de fonte por ano fique num
-- lugar só.
--
-- Cada fonte é um ramo do union, com o seu intervalo de anos. A var
-- eixo1_orcamento_federal_rcl_siconfi_desde (2018) é a fronteira: antes dela,
-- o Tesouro Transparente; dela em diante, o RREO do SICONFI, que entra num PR
-- seguinte como um segundo ramo, sem mudar este. O teste unique em ano acusa
-- se os dois ramos se sobrepuserem.
--
-- TESOURO TRANSPARENTE (2000 a 2017): "Limites da LRF para a União: Dívida
-- Consolidada Líquida", do RGF. Só o 3º quadrimestre, que acumula os 12 meses
-- do ano, e em reais (o Tesouro publica em milhares). O conjunto vai só até o
-- 2º quadrimestre de 2018, então 2018 não tem ano fechado aqui. É a RCL com que
-- o R chegou à Figura 3 de 2003 a 2017 (a planilha RCL_2011_2025 que o
-- gasto_federal.R lê começa em 2011).
--
-- SICONFI (2018 em diante, PR seguinte): RREO da União, 6º bimestre, Anexo 03,
-- conta RREO3ReceitaCorrenteLiquida, coluna 'TOTAL (ÚLTIMOS 12 MESES)', da
-- silver_rreo.
select
    ano,
    periodo as periodo_fonte,
    'tesouro_transparente' as fonte_rcl,
    receita_corrente_liquida_mil * 1000 as rcl
from {{ ref("silver_lrf_uniao_dcl") }}
where
    quadrimestre = 3
    and ano < {{ var("eixo1_orcamento_federal_rcl_siconfi_desde", 2018) }}
