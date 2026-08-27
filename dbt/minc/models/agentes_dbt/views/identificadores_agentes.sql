{{ config(materialized='view') }}

-- Views — consolida os proponentes declarados nas planilhas de LPG e PNAB numa
-- lista só. Os identificadores já chegam pseudonimizados da bronze; nada aqui
-- volta a ser documento.
--
-- id_agente_miolo só é preenchido para proponente cujo CPF veio mascarado na
-- origem — é o que sustenta o match parcial de primeiro_acesso_contemplados,
-- e vem NULL para todo o resto.

SELECT
    id_agente,
    id_agente_miolo,
    documento_mascarado,
    historico_acesso_bruto,
    programa_fomento,
    'Pessoa Física' AS tipo_proponente
FROM {{ ref('lpg_agentes_pf') }}

UNION ALL

SELECT
    id_agente,
    id_agente_miolo,
    documento_mascarado,
    historico_acesso_bruto,
    programa_fomento,
    'Pessoa Jurídica' AS tipo_proponente
FROM {{ ref('lpg_agentes_pj') }}

UNION ALL

SELECT
    id_agente,
    id_agente_miolo,
    documento_mascarado,
    historico_acesso_bruto,
    programa_fomento,
    'Coletivo' AS tipo_proponente
FROM {{ ref('lpg_agentes_coletivos') }}

UNION ALL

SELECT
    id_agente,
    id_agente_miolo,
    documento_mascarado,
    historico_acesso_bruto,
    programa_fomento,
    'Pessoa Física' AS tipo_proponente
FROM {{ ref('pnab_agentes_pf') }}

UNION ALL

SELECT
    id_agente,
    id_agente_miolo,
    documento_mascarado,
    historico_acesso_bruto,
    programa_fomento,
    'Organização' AS tipo_proponente
FROM {{ ref('pnab_agentes_pj') }}
