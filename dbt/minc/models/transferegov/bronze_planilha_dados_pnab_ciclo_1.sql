-- Bronze transferegov — cópia fiel de relatorio_gestao.planilha_dados_pnab_ciclo_1,
-- como a extracao_anexos_dag grava: as abas de pessoas e organizações das
-- planilhas do PNAB (ciclo 1), uma fatia por `tabela_origem`. Sem cast, filtro
-- nem join (ADR 0010). Quem fatia e normaliza é silver_agentes_pnab_pf e _pj.
select * from {{ source("relatorio_gestao", "planilha_dados_pnab_ciclo_1") }}  -- noqa: AM04
