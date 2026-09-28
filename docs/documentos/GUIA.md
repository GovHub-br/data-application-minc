# `docs/documentos/`

Todos os documentos produzidos para o TED nº 01/2026/SGE/SE/MINC, numa pasta só.
Cada PDF traz, na folha de identificação, a Meta e o Produto a que responde, e
todos seguem a identidade visual do Gov Hub.

**As versões aqui são as finais.** Onde houve divergência entre a versão do
repositório e a que consta do 3º Relatório Parcial, prevalece a do relatório.

| Documento | Meta · Produto | pp |
|---|---|---|
| `catalogo-de-fontes-de-dados.pdf` | Meta 01 · Produto 2 | 19 |
| `mapeamento-eixo-2-pnc.pdf` | Meta 01 · Produto 1 | 26 |
| `dicionario-de-dados.pdf` | Meta 02 · Produto 3 | 25 |
| `catalogo-de-metadados.pdf` | Meta 02 · Produto 3 | 10 |
| `modelo-conceitual-logico-fisico.pdf` | Meta 02 · Produto 3 | 28 |
| `criterios-de-qualidade-dos-dados.pdf` | Meta 02 · Produto 4 | 10 |
| `fichas-de-pipeline.pdf` | Meta 02 · P5 e Meta 03 · P3 | 13 |
| `exemplos-de-uso.pdf` | Meta 02 · P5 e Meta 03 · P3 | 10 |
| `diagramas-de-fluxos-de-dados.pdf` | Meta 02/03 · Produto 2 | 12 |
| `diretrizes-infraestrutura-trino.pdf` | Meta 02 · P2 e Meta 03 · P1 | 15 |
| `scripts-de-implantacao.pdf` | Meta 03 · Produto 3 | 11 |
| `manual-de-evolucao.pdf` | Meta 03 · Produto 3 | 12 |

## Folha de rosto

Depois da capa vem uma única folha de identificação, no mesmo formato nos doze:
instituições, projeto, meta · produto e documento. **Nenhum documento traz folha
de autores** — os créditos ficam no 3º Relatório Parcial.

## De onde vem cada um

Nove são **apurados do repositório** e regeneráveis pelo gerador em `fonte/`.

Três **não têm fonte a montante** e só se corrigem editando o próprio documento:
o mapeamento do Eixo 2, de pesquisa de Paula Ribeiro; as diretrizes de
infraestrutura para o Trino, de relatório técnico recebido; e os quatro que vêm
da série Fase 1 — catálogo de fontes, dicionário de dados, catálogo de metadados
e modelo conceitual/lógico/físico —, cuja série nunca teve gerador versionado.

Todos os doze seguem a mesma identidade visual: mesma capa, mesma faixa de
capítulo, mesmo rodapé e mesma folha de comunicação visual.

## ⚠ O gerador está atrás de três documentos

`fonte/` produz `scripts-de-implantacao`, `exemplos-de-uso` e `manual-de-evolucao`
na versão **anterior** às correções do 3º Relatório Parcial. **Regenerar hoje
desfaz as correções.**

| Documento | O que foi corrigido | Onde portar |
|---|---|---|
| `scripts-de-implantacao` | A tabela listava 6 serviços e o texto dizia "sete": faltava o Trino. Acrescentados o serviço, seu perfil próprio no Compose, as variáveis do motor e da origem do SALIC, os comandos novos do Makefile e a ressalva sobre o nome `data_warehouse` | `fonte/conteudo_05.py` |
| `exemplos-de-uso` | Sete consultas usavam `minc_cotas`, schema inexistente — a macro `generate_schema_name_for_env` faz o schema ser `cotas` no target `prod`, e nenhuma consulta rodava. Removida também uma junção entre os domínios de cotas e agentes, que não casa por usarem espaços de identidade distintos | `fonte/conteudo_06.py` |
| `manual-de-evolucao` | Declarava 4 ADRs (são 5, com o 0005 da ingestão do SALIC por Trino) e mapeava-se a Meta 04 · P3, quando responde à Meta 03 · P3 | `fonte/conteudo_07.py` |

## ⚠ Um erro conhecido, ainda não corrigido

`dicionario-de-dados.pdf` afirma que "as 561 tabelas do SALIC têm nome, schema e
testes de integridade, mas **nenhuma descrição de negócio**". Isso não é mais
verdade: os `sources_*.yml` descrevem as 561 tabelas e definem **638 das 5.064
colunas — 12,6%**, com as demais marcando explicitamente a ausência da definição
na origem. A afirmação descreve um estado anterior.

## Estes PDFs são gerados, não editados

Vale para os nove apurados: o conteúdo vem do repositório — modelos dbt,
`schema.yml`, DAGs, Dockerfiles, Compose, Makefile e ADRs. **Corrigir um número
num PDF não é a correção**: o número está errado na fonte.

| Se está errado… | Corrija em… |
|---|---|
| Descrição de modelo ou coluna | o `schema.yml` correspondente em `dbt/minc/models/` |
| Contagem de modelos, testes, tabelas | nada: é apurado. Confira o parse em `fonte/` |
| Objetivo ou periodicidade de uma DAG | a própria DAG em `dags/data_ingest/` |
| Texto narrativo de um capítulo | o `fonte/conteudo_NN.py` do documento |
| Cor, tipografia, layout | `fonte/gh-print.css` |
| Uma seta ou caixa de diagrama | o `_diagrama()` no `conteudo_08.py`; as primitivas em `fonte/diagrama.py` |

## Regerar

```bash
cd docs/documentos/fonte
python3 build.py          # todos
python3 build.py 02 04    # só os indicados
```
