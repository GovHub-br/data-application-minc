# ADR 0006 — Gate A0 parcial das silvers do SALIC

- Status: aceito parcialmente
- Data: 2026-09-03
- Escopo: Metas 3, 4 e 5 da Lei Rouanet

## Contexto

O FigJam do SALIC descreve sete perguntas de indicador, mas não fixa todas as
regras necessárias para transformar pagamentos, captações, projetos e locais
em percentuais. Foram confrontados o quadro, a IN MinC nº 10/2023, a IN MinC
nº 29/2026, o relatório FGV/OEI/MinC sobre dados de 2024 e a auditoria somente
leitura das tabelas carregadas em `salic_bronze`.

A IN nº 10/2023 regulamenta a PNAB. Suas categorias territoriais são adotadas
como taxonomia de referência solicitada pelo produto, não como obrigação legal
da Lei Rouanet. As cotas de 25%, 10% e 5% são reservas de vagas em editais da
PNAB e não serão tratadas como metas normativas da Rouanet.

## Decisões aceitas

1. `captado`, `pago`, `comprovado` e `autorizado` são medidas distintas. Fatos
   silver preservam a medida da origem; numerador e denominador de um futuro
   gold devem usar a mesma medida.
2. Pagamentos usam `dtpagamento` como data de referência. A silver preserva
   valores nulos, zero, negativos e estornos; o gold deverá declarar filtros.
3. A geografia territorial representa o local de realização, separada da
   residência do proponente e da residência do prestador. A distinção segue o
   art. 15, § 2º, da IN nº 10/2023 como referência operacional.
4. Endereço ou classificação ausente é desconhecido, nunca `false`, e deve
   permanecer visível nas medidas de cobertura.
5. As 13 categorias do art. 15 da IN nº 10/2023 são a taxonomia de território
   vulnerabilizado. A classificação só será materializada para categorias com
   fonte, granularidade, limiar, versão e vigência homologados.
6. Um projeto está liberado para execução após homologação e atendimento do
   art. 56 da IN nº 29/2026. O limiar geral de 20% tem regras específicas para
   planos anuais/plurianuais e exceções; portanto não será inferido apenas da
   soma captada. Prestação de contas começa após o encerramento da execução e o
   envio/avaliação previstos nos arts. 69 a 71, não no art. 66.

## Decisões ainda abertas

- universo, numerador, denominador, inclusões e exclusões de cada gold;
- rateio ou apenas cobertura para projetos com múltiplos locais;
- fonte e regra computável para cada categoria territorial;
- conceito de primeiro acesso e horizonte histórico da Meta 5;
- política de publicação, limiar de supressão e aprovação do DPO;
- se os percentuais PNAB aparecerão apenas como comparação não normativa ou
  serão removidos da pergunta secundária da Meta 3.

## Evidência live

- A normalização antiga forçava sete dígitos e criava chaves incorretas. O
  PRONAC real `087079` vinha de ano `08` + sequencial `7079`; a regra agora
  concatena o sequencial sem padding. A origem observada tem PRONAC de 5 a 7
  dígitos.
- `sac__abrangencia` contém 860.304 linhas e 860.304 ids distintos. A ponte
  `idprojeto` disponível recupera PRONAC para 45.022 linhas e 4.269 projetos,
  cobertura insuficiente para publicar a Meta 4.
- A view detalhada contém 4.182.025 pagamentos; todos recuperam PRONAC por
  `idpronac` na ponte auditada. Em 2024 há 602.515 linhas e R$ 2.977.665.843,84
  antes de filtros. A diferença para os cerca de 567 mil pagamentos e R$ 2,8
  bilhões da FGV confirma que o recorte do gold ainda precisa ser explicitado.

## Consequências

`fct_pagamento_profissional_rouanet` e `brg_projeto_local_execucao` são criados
como ativos `Disabled`, sem certificação. O primeiro é restrito e proibido para
RAG. O segundo não classifica vulnerabilidade nem distribui valores. Nenhum
gold é criado enquanto as decisões abertas não forem homologadas.

