-- Bronze transferegov — cópia fiel de relatorio_gestao.planilha_editais_lpg, como
-- a extracao_anexos_dag grava: a aba de editais das planilhas da LPG. Sem cast,
-- filtro nem join (ADR 0010). Quem lê é silver_editais.
select * from {{ source("relatorio_gestao", "planilha_editais_lpg") }}
