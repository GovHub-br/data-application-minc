{{ config(materialized='table') }}

-- Silver — um evento por captação de projeto audiovisual registrada pela
-- Ancine, no mesmo formato dos pagamentos do BB Ágil e dos recibos da Rouanet,
-- para entrar na espinha unificada (eventos_fomento).
--
-- POR QUE ESTA FONTE ENTRA: a pergunta da Meta 5 cita o FSA entre os
-- mecanismos, e por muito tempo se registrou que a fonte dele era "uma
-- planilha da Ancine fora do banco". Não é: `ancine.consulta` está carregada,
-- com 57.860 linhas e 17 mecanismos, de 2005 a 2026. Ela tem a mesma estrutura
-- da captação da Rouanet — `cnpj_proponente` é quem RECEBEU e
-- `cnpj_investidor` é quem aportou.
--
-- ESCOPO (decisão de negócio, tomada em 20/08/2026): entram o FSA e o fomento
-- federal ao audiovisual com repasse identificável ao proponente. Ficam de fora:
--   ART25 / ART18   são Rouanet e já entram pelo SALIC, por fonte melhor —
--                   incluí-los aqui duplicaria evento.
--   RENDIMENTOS     rendimento de aplicação financeira, não é repasse a agente.
--   CONTRAPARTIDA   dinheiro do próprio proponente.
--   OUTRAS FONTES   origem não identificada.
--   LEI ESTADUAL / LEI MUNICIPAL   não são política federal, que é o recorte
--                   da pergunta.
--   ART39 CONDECINE tributo sobre o mercado audiovisual, não fomento direto ao
--                   agente. Medido: acrescentaria 46 proponentes e apenas 2
--                   correções — custo de defesa alto, ganho nulo.
--
-- DOIS MECANISMOS, NÃO UM: 'FSA' responde literalmente ao que o documento da
-- meta nomeia; 'AUDIOVISUAL' carrega Lei do Audiovisual (ART1/ART3/1A/3A),
-- FUNCINES e os editais da Ancine. Colapsar tudo em "FSA" seria nome errado
-- para a maior parte das linhas.
--
-- ARMADILHAS DE FORMATO, as duas medidas nesta tabela:
--   data_captacao      TEXT no padrão AMERICANO M/D/YYYY. '7/12/2007' é 12 de
--                      julho, não 7 de dezembro. Ordenar como texto mente.
--   valor_de_captacao  TEXT no padrão brasileiro, '50.000,00'.
-- Verificado no escopo acima: 0 datas e 0 valores fora do padrão, 0 negativos.
--
-- DOCUMENTO: a tabela é quase toda CNPJ (31.528 linhas de 14 dígitos) e tem 4
-- linhas com CPF. Proponente pessoa física é, na prática, invisível aqui — é a
-- limitação desta fonte, e ela empurra na mesma direção de todas as outras:
-- pode deixar de revelar um veterano, nunca inventar um.

WITH bruto AS (
    SELECT
        CASE
            WHEN mecanismo = 'FSA' THEN 'FSA'
            ELSE 'AUDIOVISUAL'
        END AS programa_fomento,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(cnpj_proponente), '[^0-9]', '', 'g')) <= 11
                THEN LPAD(REGEXP_REPLACE(TRIM(cnpj_proponente), '[^0-9]', '', 'g'), 11, '0')
            ELSE LPAD(REGEXP_REPLACE(TRIM(cnpj_proponente), '[^0-9]', '', 'g'), 14, '0')
        END AS beneficiario_documento,
        NULLIF(TRIM(nome_proponente), '') AS beneficiario_nome,
        TO_DATE(data_captacao, 'FMMM/FMDD/YYYY') AS data_evento,
        REPLACE(REPLACE(valor_de_captacao, '.', ''), ',', '.')::NUMERIC AS valor,
        NULLIF(TRIM(no_salic), '') AS no_salic
    FROM {{ source('ancine', 'consulta') }}
    WHERE mecanismo IN (
              'FSA',
              'ART1', 'ART3', 'ART 1A', 'ART 3A',
              'ART41 (FUNCINES)',
              'EDITAL ANCINE', 'EDITAL ANCINE (PAR)', 'OUTROS EDITAIS'
          )
      AND cnpj_proponente IS NOT NULL
      AND data_captacao ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
      AND valor_de_captacao ~ '^-?[0-9.]+,[0-9]{2}$'
      AND LENGTH(REGEXP_REPLACE(TRIM(cnpj_proponente), '[^0-9]', '', 'g')) BETWEEN 8 AND 14
)

SELECT
    beneficiario_documento,
    beneficiario_nome,
    programa_fomento,
    data_evento,
    valor,
    no_salic
FROM bruto
WHERE beneficiario_documento !~ '^0+$'
  -- captação com valor zero ou negativo não deu acesso a recurso, mesmo
  -- critério aplicado à Rouanet
  AND valor > 0
