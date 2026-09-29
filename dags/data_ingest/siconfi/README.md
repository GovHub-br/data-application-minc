# Ingestão SICONFI

A DAG `siconfi_ingestion_dag` extrai da [API de Dados Abertos do SICONFI](https://apidatalake.tesouro.gov.br/docs/siconfi.yaml)
(Tesouro Nacional) para o schema `siconfi_bronze`:

| Endpoint | O que traz | Desde |
|---|---|---|
| `/anexos-relatorios` | lista oficial de `no_anexo` por demonstrativo e esfera | — |
| `/entes` | cadastro e população dos 5.598 entes (foto do ano corrente) | — |
| `/extrato_entregas` | o que cada ente entregou em cada ano, e em que situação | 2013 |
| `/dca` | Balanço Anual | 2013 |
| `/rreo` | Relatório Resumido de Execução Orçamentária, por bimestre | 2015 |
| `/rgf` | Relatório de Gestão Fiscal, por quadrimestre ou semestre e por poder | 2015 |
| `/msc_orcamentaria` | Matriz de Saldos Contábeis, classes 5 e 6 | 2019 |

`/msc_patrimonial` (classes 1–4) e `/msc_controle` (7–8) continuam suportados,
**desligados por padrão**: estão fora do escopo do datalake de análise.

O código está dividido em três módulos de `plugins/`:

- `cliente_siconfi.py`: HTTP e regras da API;
- `siconfi_config.py`: configuração;
- `siconfi_storage.py`: fila e bronze.

A DAG só orquestra.

## Antes de mexer: a API responde vazio, não erro

Parâmetro faltando ou inválido **não gera erro**: a API responde HTTP 200 com
`items: []`. Por isso:

- a configuração é validada antes de qualquer chamada, e a DAG falha com a
  lista de todos os problemas;
- toda consulta passa por `validate_params` antes de sair;
- o **extrato de entregas decide o que se busca** de cada ente e ano:
  - RREO normal ou simplificado;
  - RGF quadrimestral (`RGF`) ou semestral (`RGF Simplificado`), e de qual poder;
  - os meses de MSC entregues;
  - se a DCA foi entregue.

Cada partição processada fica em `siconfi_control.partition_log` com um de
quatro status:

| Status | Quando |
|---|---|
| `sucesso_com_dados` | a busca trouxe linhas |
| `vazio_esperado` | veio vazio e o extrato não mostra a entrega (por exemplo, o extrato de um ente sem entregas no ano, ou um poder de RGF chutado porque a instituição não foi reconhecida) |
| `vazio_inesperado` | o extrato mostra a entrega e a API não devolveu nada. **É aqui que aparece bug de parâmetro.** Não volta sozinho para a fila |
| `erro` | falha HTTP, parâmetro recusado pela validação ou tentativas esgotadas |

Consulta útil depois de uma execução:

```sql
SELECT endpoint, status, count(*)
FROM siconfi_control.partition_log
WHERE run_id = '<run_id>'
GROUP BY 1, 2 ORDER BY 1, 2;
```

A configuração efetiva de cada execução fica em `siconfi_control.run_config`:
já mesclada e com `"corrente"` resolvido, junto com a Variable e o
`dag_run.conf` que a produziram.

## Configuração

Tudo o que se extrai vem da Variable **`siconfi_extracao_config`** (JSON). A
precedência é:

```text
dag_run.conf  >  Variable siconfi_extracao_config  >  padrões do código
```

- **A mescla é profunda.** Informe só o que muda; o resto é herdado da camada
  de baixo.
- **Listas e `null` substituem.** Não se somam ao valor de baixo.
- **Sem a Variable, a DAG funciona** com os padrões.
- **Os padrões ficam em dois lugares idênticos:**
  [`siconfi_extracao_config.json`](siconfi_extracao_config.json) e o
  `DEFAULT_CONFIG` de `plugins/siconfi_config.py`. Um teste garante que são
  iguais.

> A Variable antiga `siconfi_config` (chaves `start_year`, `fact_endpoints`...)
> não é mais lida. Se ela existir na instância, apague-a depois de criar a nova.

### Criar ou atualizar a Variable

Pela interface: **Admin → Variables → +**, chave `siconfi_extracao_config`,
valor o JSON.

Pela linha de comando, dentro do container do Airflow:

```bash
airflow variables set --serialize-json siconfi_extracao_config \
  "$(cat dags/data_ingest/siconfi/siconfi_extracao_config.json)"

# ou, para importar de um arquivo {"nome_da_variable": valor}:
jq '{siconfi_extracao_config: .}' dags/data_ingest/siconfi/siconfi_extracao_config.json \
  > /tmp/siconfi_variable.json
airflow variables import /tmp/siconfi_variable.json
```

A Variable não precisa ter todas as chaves. Esta, por exemplo, só liga a Fase 2
da MSC:

```json
{"endpoints": {"msc_orcamentaria": {"meses": [1,2,3,4,5,6,7,8,9,10,11,12]}}}
```

### `global`

| Campo | Padrão | O que faz |
|---|---|---|
| `esferas` | `["M","E","D","U"]` | esferas dos entes extraídos. **O DF é `D`**: quem deixa só `E` perde o DF |
| `incluir_cod_ibge` | `[]` | vazio = todos os entes das esferas; com códigos, só eles. Útil para teste e reprocessamento |
| `excluir_cod_ibge` | `[]` | entes que nunca são buscados |
| `requisicoes_por_segundo` | `1` | ritmo máximo, somado entre todas as tasks (0 < x ≤ 3). A 1 req/s o intervalo é 1,02 s, como no código legado |
| `max_paralelismo` | `2` | chamadas em andamento ao mesmo tempo, somadas entre todas as tasks (1–5). Não se sabe como a API reage ao excesso: suba monitorando o `erro` no `partition_log` |
| `retentativas` | `4` | tentativas HTTP por chamada, com backoff. Cobre a resposta momentânea que não é JSON |
| `tamanho_pagina` | `5000` | `limit` de cada página (máximo da API) |
| `max_minutos_por_execucao` | `45` | quanto cada task de ingestão trabalha antes de devolver o resto à fila (< 60, o lease da fila) |
| `max_particoes_por_execucao` | `2000` | teto de partições reservadas por task |
| `max_tentativas_por_particao` | `5` | execuções seguidas com erro transitório até a partição virar `erro` |
| `max_extratos_por_planejamento` | `20000` | extratos (ente × ano) lidos por execução do planejamento |
| `reprocessar` | `[]` | **só pelo `dag_run.conf`**: devolve à fila as partições do recorte nesses status (`vazio_inesperado`, `erro`, `vazio_esperado`, `sucesso_com_dados`). Na Variable é recusado, porque rebuscaria tudo a cada hora |

### `endpoints`

Todos os blocos aceitam `ativo`. `ano_fim` aceita um ano ou `"corrente"`,
resolvido a cada execução. `no_anexo: null` significa todos os anexos numa
chamada só, o que é preferível. Uma lista gera uma chamada por anexo, e cada
nome é conferido contra `/anexos-relatorios`.

| Bloco | Campos próprios |
|---|---|
| `anexos_relatorios`, `entes` | `recarga_horas` (168): intervalo mínimo entre recargas. Cada recarga de `/entes` fica guardada, o que dá o histórico de população e cadastro |
| `extrato_entregas` | `ano_inicio` (≥ 2013), `ano_fim`. Os anos de **todo** demonstrativo ligado precisam caber aqui: o que não está no extrato nunca é planejado. `rebusca_anos` (2) e `rebusca_dias` (7): o extrato desses exercícios volta à fila nesse intervalo, e é assim que retificações chegam |
| `dca` | `ano_inicio` (≥ 2013), `ano_fim`, `no_anexo`. Em 2013 o anexo se chama `Anexo I-X`, sem o prefixo `DCA-`: a DAG traduz sozinha |
| `rreo` | `ano_inicio` (≥ 2015), `ano_fim`, `periodos` (bimestres 1–6), `no_anexo` |
| `rgf` | `ano_inicio` (≥ 2015), `ano_fim`, `no_anexo`, `poderes_por_esfera`. Precisa ter todas as esferas de `global.esferas`; município é só `E` e `L`. Fora do Executivo, só os anexos 01, 05 e 06 são pedidos |
| `msc_orcamentaria` | `ano_inicio` (≥ 2019), `ano_fim`, `classes` (⊂ {5, 6}), `meses` (1–12), `tipos_matriz` (⊂ {MSCC, MSCE}; MSCE só com 12 em `meses`, e sempre consultada com `me_referencia=12`), `id_tv` (⊂ {beginning_balance, ending_balance, period_change}) |
| `msc_patrimonial`, `msc_controle` | os mesmos da `msc_orcamentaria`, com as classes 1–4 e 7–8 |

## Como a DAG trabalha

```text
carregar_configuracao → atualizar_referencias → montar_particoes_extrato
  → ingerir_extrato_entregas → montar_particoes_fatos → ingerir_<endpoint> (em paralelo)
```

- **Cada partição é uma chamada**: endpoint mais todos os parâmetros. Ela fica
  numa fila em `siconfi_control.work_queue`, e o Airflow não expande uma task
  por partição, porque são centenas de milhares.
- **As tasks de ingestão trabalham por tempo** (`max_minutos_por_execucao`) e
  devolvem o resto. A execução seguinte, de hora em hora, continua de onde a
  anterior parou.
- **O planejamento lê o extrato de um ente e ano por vez.** Assembleia e
  Tribunal de Contas entregam RGF separados e ambos respondem em `co_poder=L`:
  viram uma consulta só.
- **Retificações.** O marcador de revisão de cada consulta combina
  `data_status`, `status_relatorio` e `forma_envio` de todas as linhas do
  extrato que a geraram. O demonstrativo só é rebuscado quando esse marcador
  muda.
- **Bronze.** Cada busca ganha um `fetch_id`, e só a busca completa mais
  recente de cada consulta vale. O detalhe está no docstring de
  `plugins/siconfi_storage.py`.

### Cargas em fases

Cada fase é uma mudança na Variable:

1. **Referências e extrato:** deixe só `anexos_relatorios`, `entes` e
   `extrato_entregas` ligados.
2. **DCA e MSC Fase 1:** ligue `dca` e `msc_orcamentaria` com `"meses": [12]`,
   que é o padrão.
3. **RREO e RGF:** ligue `rreo` e `rgf`.
4. **MSC Fase 2:** passe `msc_orcamentaria.meses` para `[1, ..., 12]`. As
   partições de dezembro já feitas não são refeitas.

Para um backfill ou reprocessamento pontual, dispare a DAG com `dag_run.conf`.
Exemplo: refazer os vazios inesperados da MSC do ES.

```json
{"global": {"incluir_cod_ibge": [32], "reprocessar": ["vazio_inesperado"]}}
```

O `dag_run.conf` vale só para aquela execução. O que ela não terminar fica na
fila e só é retomado por uma execução cujo recorte o inclua. Para cargas
grandes, mude a Variable.
