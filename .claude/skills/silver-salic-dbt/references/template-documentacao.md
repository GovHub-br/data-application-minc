# Documentação — o padrão obrigatório

**Esta é a parte que importa.** O cast é mecânico; o significado não. Você está
com o banco aberto — é a única hora barata de descobrir o que a tabela quer
dizer. Descrição preguiçosa aqui custa meses depois.

## O template

```yaml
version: 2

models:
  - name: perfil_agentes_completo
    description: >
      Uma linha por agente cultural. Consolida o histórico de acesso a fomento
      do LPG e do PNAB e classifica o agente entre primeiro acesso e recorrente.

    config:                                   # selecionável: dbt build --select tag:gold
      tags: [gold, agentes, minc]             # → chegam ao OM como dbtTags.*

    meta:
      openmetadata:
        tier: "Tier.Tier1"
        owner: "arthrok"
        domain: "Cultura"
        tags: ["Certification.Gold"]
        glossary:
          - "MinC.AgenteCultural"
          - "MinC.PNAB"
        customProperties:
          responsavel_negocio: "Meta 5 - agentes"

    columns:
      - name: identificador_unico
        description: >
          CPF ou CNPJ do agente, normalizado sem pontuação. Chave de ligação
          com as tabelas de contemplados e de pagamentos.
        data_type: text
        meta:                                 # meta.openmetadata vale em COLUNA também
          openmetadata:
            glossary: ["MinC.IdentificadorAgente"]
        tests:
          - not_null
          - unique

      - name: programa_fomento
        description: Política que originou o acesso, LPG ou PNAB.
        data_type: text
        tests:
          - accepted_values:
              arguments:                      # ← dbt 1.10
                values: ["lpg", "pnab"]

    tests:                                    # alinhado com `columns` = teste de TABELA
      - row_count_match:
          arguments:
            source_table: agentes.lpg_agentes_pf
            target_table: agentes.perfil_agentes_completo
```

## ⚠ Um ajuste que o repositório exige

O template mostra `meta:` no topo do modelo. **Neste repositório, em MODELO o
`meta` fica sob `config`** — é o que `dbt/README.md` documenta e o que todos os
`schema.yml` existentes fazem:

```yaml
    config:
      tags: [silver, salic, sac]
      meta:
        openmetadata:
          tier: "Tier.Tier3"
          domain: "Cultura.Incentivo Fiscal"
          owner: "minc-data-engineering"
```

Declarar nos dois lugares **aborta o parse no dbt 1.10**. Em **coluna** e em
**source**, o `meta` continua no topo, como no template. Siga o resto do
template à risca.

## A régua das descrições

Uma descrição boa responde o que o nome da coluna **não** responde. Se ela só
reescreve o nome, não escreva — deixe o espaço vazio, que é mais honesto.

**Na tabela, diga sempre:**
- **O grão.** "Uma linha por X." É a informação mais usada e a mais ausente.
- **O que ela significa** no negócio do SALIC/Rouanet — não o que o nome sugere.
- **Volume medido** e recorte temporal, quando houver.
- **A armadilha**, se existir. Coluna que mente, valor sentinela, duplicata,
  janela incompleta. Isto vale mais que todo o resto.
- **Como cruza** com outras tabelas: qual chave, e se o casamento é exato.

**Na coluna, diga:**
- O que o valor **significa**, não o tipo (o tipo já está em `data_type`).
- **Preenchimento e cardinalidade** medidos: "preenchida em 79,6%, 3.406
  distintos".
- **O domínio de valores**, quando for pequeno — e a contagem de cada um.
- **O formato**, quando for traiçoeiro: data em padrão americano, valor em
  padrão brasileiro, documento com máscara.
- **O que virou NULL no cast**, e quantas linhas.

**Marque explicitamente o que está degradado.** Estes prefixos são convenção
viva nas sources deste repositório e devem ser reusados:

```
[VAZIA EM 100% DAS LINHAS]
[CONSTANTE 'X']
[INUTILIZÁVEL — literal 'NaN' em todas as linhas]
[REDUNDANTE — idêntico a outra_coluna]
[NÃO VERIFICADO]
```

Na **silver**, o `[NÃO VERIFICADO]` não vale: a guarda de governança reprova.
Escreva em prosa o que foi medido e o que não se sabe.

**Não invente.** Se você não sabe o que a coluna significa, escreva o que
**mediu** e diga que a semântica não foi confirmada. Descrição inventada é pior
que ausência, porque ninguém sabe que precisa conferir.

## Exemplo do nível esperado

```yaml
      - name: dt_recibo
        description: >
          Data do recibo de captação, já convertida do TEXT da origem.
          Preenchida em 99,4% das 8.330.253 linhas. 412 linhas (0,005%) não
          casaram com o padrão ISO e viraram NULL no cast — todas com o
          literal '1900-01-01', que é o sentinela de "sem data" do SQL Server,
          não uma data real. Série de 1993 a 2026.
        data_type: timestamp
```

Compare com o que **não** serve: *"Data do recibo."*

## Glossário

Termo de negócio cuja ambiguidade muda a leitura do número vai para
`helpers/openmetadata/glossaries/minc.csv` e é referenciado por FQN
(`MinC.Identificadores.PRONAC`). Referenciar **não cria** o termo.

Antes de referenciar, confirme que existe:

```bash
python3 -c "
import csv
r=csv.DictReader(open('helpers/openmetadata/glossaries/minc.csv'))
print(sorted((x['parent']+'.'+x['name']) if x['parent'] else 'MinC.'+x['name'] for x in r))
"
```

O SALIC já tem termos úteis: `MinC.Identificadores.PRONAC`,
`MinC.Politicas.Rouanet`, `MinC.Agentes.Proponente`, `MinC.Agentes.Investidor`.

## Tier, domínio e tags

| Objeto | Tier | Onde declara |
|---|---|---|
| Bronze (`salic.bronze_*`, source) | `Tier.Tier4` | `meta` da source em `sources_<banco>.yml` |
| Silver migrada, sem consumo a jusante | `Tier.Tier3` | `config.meta` do modelo em `schema_<banco>.yml` |
| Silver consumida por produto de dados | `Tier.Tier2` | idem; é o caso de `silver_sac_captacao_proponente` |

```yaml
tags: [silver, salic, <banco>]        # config.tags do modelo
domain: "Cultura.Incentivo Fiscal"    # SALIC/Rouanet
owner: "minc-data-engineering"
```

Domínios em uso: `Cultura.Incentivo Fiscal`, `Cultura.Fomento Direto`,
`Cultura.Repasse Federativo`, `Cultura.Cadastro Cultural`. As silvers que o
script de lote migra chegam sem `config` de modelo: acrescentar faz parte da
análise do lote.

## Governança (obrigatória)

Toda silver do SALIC declara, em `config.meta`, o bloco que
`tests/test_salic_silver_governance.py` exige:

```yaml
    config:
      tags: [silver, salic, <banco>]
      meta:
        status: Disabled                 # Active só depois dos gates G1–G5
        origem_dados: 'salic.bronze_<banco>_<tabela>'
        governance:
          produto_dados: 'SALIC — Rouanet'
          classificacao_uso: restrito    # publico | interno | restrito
          rag_publication: prohibited    # ou eligible_after_security_validation
        openmetadata:
          tier: "Tier.Tier3"
          domain: "Cultura.Incentivo Fiscal"   # nunca Cultura.Fomento Direto
          owner: "minc-data-engineering"
```

Modelo com qualquer coluna de PII é `restrito` e `prohibited`: a guarda
reprova PII em modelo elegível ao RAG.

## Dado pessoal e credencial

Tag de coluna em `meta.openmetadata.tags`:

| Coluna | Tag |
|---|---|
| CPF, CNPJ de pessoa física, data de nascimento, e-mail, telefone, endereço, raça, deficiência | `PII.Sensitive` |
| nome de pessoa, sem outro identificador na mesma coluna | `PII.NonSensitive` |
| chave que leva a uma pessoa: `idusuario`, `idagente`, `idproponente` | `PII.Sensitive` |
| senha, hash de senha, token, chave | **não entra na silver**: sai do `select` |

A descrição de coluna sensível diz o **formato** ("CPF com 11 dígitos, sem
máscara, preenchido em 100%"), nunca um exemplo de valor.

## `meta.used`

Se nenhum modelo consome a tabela, marque `meta: {used: false}`. É convenção
viva em `agentes`, `bbagil`, `dados_salic` e `transferegov`, e evita que o
catálogo pareça um pipeline que não existe.
