-- Bronze transferegov — cópia fiel de
-- relatorio_gestao.planilha_contemplados_pnab_ciclo_1.
-- A tabela é uma planilha anexada pelo ente ao relatório de gestão do plano de
-- ação no TransfereGov Fundo a Fundo, lida pela extracao_anexos_dag; o schema
-- `relatorio_gestao` é onde essa DAG grava.
-- Habilitada pela migração da Meta 3 (cotas): silver_contemplados_pnab e
-- silver_edital_ano_por_arquivo leem daqui. silver_planilha_contemplados (Meta 5)
-- continua enabled=false por decisão própria, ver lá.
select * from {{ source("relatorio_gestao", "planilha_contemplados_pnab_ciclo_1") }}
