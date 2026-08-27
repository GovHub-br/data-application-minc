{{ config(materialized='table') }}

-- Bronze — extrato bancário do BB Gestão Ágil (schema bbagil), filtrado para
-- manter só pagamentos reais a beneficiários finais. Porta pra SQL os 8
-- filtros sequenciais de plugins/regras_negocio_bbagil.py::pipeline_filtro_extrato
-- (módulo órfão em Python, mantido só como referência/histórico dessas regras):
--   1. beneficiário válido (documento != '0') e sem subtransações (as com
--      subtransactionquantity > 0 são tratadas em bbagil_subtransacoes_filtrado,
--      pra não contar o mesmo pagamento 2x).
--   2. remove transferências para o próprio Banco do Brasil (documento '191').
--   3. remove transações internas/impostos/aplicações automáticas (descriptionname).
--   4. remove devoluções de saldo ao Fundo Nacional de Cultura (CNPJ 3793086100189).
--   5. remove pares crédito/débito do mesmo plano de ação + documento + valor
--      (estorno no mesmo dia ou devolução futura de um débito anterior).
--   6. mantém só débitos (saída de dinheiro da conta do ente pro beneficiário).
--   7. remove repasses entre entes públicos (nome do beneficiário começa com
--      MUNICIPIO/ESTADO/FUNDO/SECRETARIA/SEFAZ — são repasses administrativos,
--      não pagamento a agente cultural).
--
-- O documento é canonizado via beneficiarypersontype (1=PF/CPF, 2=PJ/CNPJ) +
-- LPAD, porque uma fração relevante das linhas chega sem o zero à esquerda
-- (CNPJ com 12-13 dígitos, CPF com 10) — e só então vira pseudônimo. O
-- documento em claro existe apenas dentro deste modelo, para os filtros que
-- dependem dele; o que sai é id_beneficiario. Ver macros/hash_documento.sql.
--
-- beneficiario_nome NÃO sai daqui. Ele existe só para o filtro 7, que é
-- aplicado no WHERE abaixo: usa-se e descarta-se no mesmo modelo. Hashear nome
-- seria pior que inútil — não serve de chave e uma tabela de nomes é trivial de
-- montar. tipo_pessoa é materializado aqui porque, depois do hash, ninguém
-- consegue mais deduzi-lo do comprimento do documento.

WITH base AS (
    SELECT
        id_plano_acao,
        id,
        governmentprogramname AS programa_curto,
        beneficiarydocumentid AS beneficiario_documento_bruto,
        {{ documento_canonico('beneficiarydocumentid', 'beneficiarypersontype') }} AS documento_canon,
        beneficiaryname AS beneficiario_nome,
        creditdebitindicator,
        value::NUMERIC AS valor,
        TO_DATE(bookingdate, 'DD/MM/YYYY') AS data_pagamento,
        descriptionname
    FROM {{ source('bbagil', 'extrato_bbagil') }}
    WHERE subtransactionquantity = '0'
      AND beneficiarydocumentid IS NOT NULL
      AND beneficiarydocumentid NOT IN ('0', '191')
),

-- Chaves (plano de ação + documento bruto + valor) que aparecem como débito
-- E crédito — estorno/devolução. Descarta os dois lados do par.
chaves_estorno AS (
    SELECT id_plano_acao, beneficiario_documento_bruto, valor
    FROM base
    WHERE creditdebitindicator = 'D'
    INTERSECT
    SELECT id_plano_acao, beneficiario_documento_bruto, valor
    FROM base
    WHERE creditdebitindicator = 'C'
)

SELECT
    b.id_plano_acao,
    b.id,
    b.programa_curto,
    {{ hash_documento('b.documento_canon') }} AS id_beneficiario,
    CASE
        WHEN LENGTH(b.documento_canon) = 11 THEN 'PF'
        ELSE 'PJ'
    END AS tipo_pessoa,
    b.valor,
    b.data_pagamento
FROM base b
LEFT JOIN chaves_estorno ce
    ON b.id_plano_acao = ce.id_plano_acao
    AND b.beneficiario_documento_bruto = ce.beneficiario_documento_bruto
    AND b.valor = ce.valor
WHERE b.creditdebitindicator = 'D'
  AND ce.id_plano_acao IS NULL
  AND b.documento_canon IS NOT NULL
  AND b.beneficiario_documento_bruto != '3793086100189'
  AND b.descriptionname NOT IN (
      'BB-APLIC C.PRZ-APL.AUT', 'Resgate Automatico', 'Impostos',
      'ORDEM BANC CANCELADA', 'Ordem Bancaria', 'Resgate BB Fix',
      'CREDITO CONVENIO', 'Estorno Resgate Automatico'
  )
  AND NOT (
      UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'MUNICIPIO%'
      OR UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'ESTADO%'
      OR UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'FUNDO%'
      OR UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'SECRETARIA%'
      OR UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'SEFAZ%'
  )
