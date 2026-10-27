-- Bronze transferegov — cópia fiel de relatorio_gestao.planilha_dados_lpg, como a
-- extracao_anexos_dag grava: as abas de dados (perfil de PF, PJ, coletivos e
-- instrumentos) das planilhas que os entes anexam ao relatório de gestão da LPG,
-- uma fatia por `tabela_origem`. Sem cast, filtro nem join (ADR 0010).
-- Quem fatia e normaliza é silver_agentes_pf, _pj, _coletivos e silver_editais.
select * from {{ source("relatorio_gestao", "planilha_dados_lpg") }}  -- noqa: AM04
