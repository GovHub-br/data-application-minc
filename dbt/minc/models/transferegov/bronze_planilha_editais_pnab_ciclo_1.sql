-- Bronze transferegov — cópia fiel de relatorio_gestao.planilha_editais_pnab_ciclo_1,
-- como a extracao_anexos_dag grava: as abas de definição de edital do PNAB
-- (ações gerais e Cultura Viva), uma fatia por `tabela_origem`. Sem cast,
-- filtro nem join (ADR 0010). Quem lê é silver_edital_ano_por_anexo.
select * from {{ source("relatorio_gestao", "planilha_editais_pnab_ciclo_1") }}
