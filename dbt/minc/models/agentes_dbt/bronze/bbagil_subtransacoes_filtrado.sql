{{ config(materialized='table') }}

-- Bronze — sublançamentos do BB Gestão Ágil (schema bbagil), usados quando a
-- transação-mãe do extrato tem subtransactionquantity > 0 (o beneficiário
-- real do pagamento só aparece aqui, não na linha agregada do extrato).
-- Porta pra SQL os 5 filtros de
-- plugins/regras_negocio_bbagil.py::pipeline_filtro_subtransacoes:
--   1. beneficiário válido (documento != '0').
--   2. mantém só sublançamentos com status 'Pago' (accountability).
--   3. valor em módulo (subtransação pode vir negativa na fonte).
--   4. remove repasses entre entes públicos (mesmo critério do extrato).
--
-- Contexto de programa (governmentprogramname) vem herdado da
-- transação-mãe via id_plano_acao + id_transacao_pai = id.

SELECT
    s.id_plano_acao,
    e.governmentprogramname AS programa_curto,
    CASE
        WHEN s.beneficiarypersontype = '1'
            THEN LPAD(REGEXP_REPLACE(s.beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
        WHEN s.beneficiarypersontype = '2'
            THEN LPAD(REGEXP_REPLACE(s.beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
    END AS beneficiario_documento,
    s.beneficiaryname AS beneficiario_nome,
    ABS(s.value::NUMERIC) AS valor,
    TO_DATE(s.paymentdate, 'DD/MM/YYYY') AS data_pagamento
FROM {{ source('bbagil', 'subtransacao_bbagil') }} s
JOIN {{ source('bbagil', 'extrato_bbagil') }} e
    ON s.id_plano_acao = e.id_plano_acao
    AND s.id_transacao_pai = e.id
WHERE s.beneficiarydocumentid IS NOT NULL
  AND s.beneficiarydocumentid != '0'
  AND s.subtransactionaccountabilityname = 'Pago'
  AND NOT (
      UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'MUNICIPIO%'
      OR UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'ESTADO%'
      OR UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'FUNDO%'
      OR UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'SECRETARIA%'
      OR UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'SEFAZ%'
  )
