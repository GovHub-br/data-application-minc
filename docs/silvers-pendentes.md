# Silvers pendentes do SALIC — especificação

Os seis modelos que faltam para completar as 14 silvers previstas para as Metas
3, 4 e 5 da Lei Rouanet. Oito já existem em
[`dbt/minc/models/salic_dbt/`](../dbt/minc/models/salic_dbt/).

Cada bloco abaixo tem o formato do `description` que vai para o `schema.yml`
quando o modelo for escrito — grão, universo, para que serve, desconhecidos,
limitações. A intenção é que a especificação seja movida para lá, não
reescrita.

> Este documento descreve modelos que **ainda não existem**. Um `schema.yml`
> que referencia modelo sem `.sql` quebra o `dbt parse`, e é por isso que a
> especificação mora aqui até o modelo nascer.

## O que mudou em 2026-09-04

A ingestão do dia trouxe 18 tabelas novas ao `salic_bronze`, e três bloqueios
descritos no `HANDOFF.md` mudaram de natureza. O que era "falta ingerir" virou
"falta permissão de leitura" ou "falta uma decisão pequena".

| Antes | Agora |
|---|---|
| núcleo do banco `Agentes` só na v1 | `agentes__agentes`, `agentes__endereconacional`, `agentes__tbagentefisico`, `agentes__municipios` estão no `salic_bronze` |
| `sac__projetos` só na v1 | está no `salic_bronze` com 277.187 linhas |
| `sac__preprojeto` não ingerida | está no `salic_bronze` com 449.580 linhas |
| `territorio_fcu_setores` não ingerida | está em `transferegov` com 33.272 linhas |
| sem coluna de autodeclaração em lugar nenhum | `tbAgenteFisico` tem `stCorRaca` e `stNecessidadeEspecial` |

**Bloqueio comum a quase todos:** as 18 tabelas novas nasceram sem `SELECT`
para a role de analytics. Enquanto o `GRANT` não vier, nem a medição nem o
`dbt build` conseguem lê-las.

---

## 1. `dim_municipio_ibge`

**Grão:** um município brasileiro.
**Universo:** os 5.568 municípios de `agentes__municipios`, mais Distrito
Federal e os territórios que a fonte trouxer.

**Para que serve:** dimensão territorial de referência. Metade da Meta 4
depende dela — sem código IBGE conformado, `brg_projeto_local_execucao` não
liga local de execução a território classificado.

**Colunas previstas**

Estrutura das fontes, conferida na origem em 04/09.
`salic_agentes.dbo.municipios`:

```
idmunicipioibge   varchar(6)     todas as 5.568 linhas com 6 caracteres
idufibge          integer
idmeso            char(4)
idmicro           char(5)
descricao         varchar(100)
```

`salic_agentes.dbo.uf`: `iduf`, `sigla` char(2), `descricao`, `regiao`.

| coluna | origem |
|---|---|
| `codigo_ibge` | `agentes__municipios.idmunicipioibge` |
| `nome_municipio` | `agentes__municipios.descricao` |
| `id_uf_ibge` | `agentes__municipios.idufibge` |
| `sigla_uf`, `nome_uf`, `regiao` | `agentes__uf` |
| `mesorregiao`, `microrregiao` | `agentes__municipios` |
| `eh_capital` | **seed** — 27 linhas, escrito à mão |

**Armadilha do código IBGE.** `idmunicipioibge` tem 6 posições em todas as
5.568 linhas: é o código **sem o dígito verificador**. O código completo do
IBGE tem 7, e é a forma que o `transferegov` usa
(`codigo_ibge_fundo_programa` = `5300108`). Cruzar as duas formas sem tratar o
tamanho não casa nenhuma linha — e falha em silêncio, porque join que não
encontra não dá erro.

A dimensão deve expor a chave de 6 posições, e quem vier com 7 trunca a última
antes de juntar. Calcular o dígito verificador é possível, mas é regra a mais
para manter sem necessidade.

**Desconhecidos:** município sem código IBGE na origem sai com `codigo_ibge`
nulo e não é descartado — cobertura tem de permanecer visível.

**Bloqueio atual:** `GRANT SELECT` em `salic_bronze.agentes__municipios` e
`agentes__uf`, mais o seed de capitais. Nenhum bloqueio de dados ou de decisão.
As duas sources já estão declaradas em `sources_agentes.yml`.

**É o mais barato dos seis** e destrava outros dois.

---

## 2. `dim_agente_perfil_rouanet_scd`

**Grão:** um agente por intervalo de validade (slowly changing dimension).
**Universo:** os agentes de `agentes__agentes` (997.756) que têm registro em
`agentes__tbagentefisico` (136.607).

**Para que serve:** os indicadores de diversidade da Meta 3.

**Estado da fonte — corrige o Bloqueio 1 do HANDOFF.** O handoff afirma que
não existe coluna de raça, etnia ou deficiência em nenhuma das 1.121 tabelas, e
conclui que os indicadores não são calculáveis a partir do SALIC. Medido na
origem via Trino em 04/09, `salic_agentes.dbo.tbAgenteFisico`:

```
136.607  linhas
 31.405  stCorRaca preenchida          7 valores distintos
136.607  stNecessidadeEspecial         2 valores distintos  (100%)
```

Domínio observado de `stCorRaca`:

```
NULL   105.202      não preenchido
   1    17.504
   3     7.079
   2     3.963
 ' '     2.297      espaço em branco
   4       313
   5       227
   u        22      lixo
```

São **códigos**, não rótulos. Os cinco válidos batem em quantidade com as cinco
categorias do IBGE, e a distribuição é plausível — mas o mapeamento não está
confirmado.

**Cobertura:** 29.086 autodeclarações utilizáveis (códigos 1 a 5), o que dá
21,3% da tabela e **2,9% dos 997.756 agentes**. Deficiência tem cobertura
total.

**Por que o handoff não encontrou:** `stcorraca` só aparece se o padrão `raca`
não exigir fronteira de palavra — não há fronteira entre `cor` e `raca`. É a
mesma armadilha documentada em
[`scripts/classificar_pii.py`](../scripts/classificar_pii.py). E
`stnecessidadeespecial` não contém a palavra "deficiência", então nenhum padrão
semântico a alcançaria.

**Bloqueio atual.** A categoria legível de `stCorRaca` virá por
**enriquecimento da base de origem**, decidido em 2026-09-04 — não por tabela
de domínio a ser descoberta nem por seed neste projeto. Procurei domínio no
catálogo `salic_agentes` e não existe.

Fica pendente também a decisão de produto sobre publicar indicador com 2,9% de
cobertura em raça, que é conversa com quem definiu a meta.

**Governança:** contém dado sensível pelo art. 5º II da LGPD. Classificação de
uso restrita, **proibido para RAG**, sem exceção.

**Indígena e quilombola continuam sem fonte.** Indígena provavelmente é uma das
categorias de `stCorRaca`; quilombola não tem coluna própria em lugar nenhum.

---

## 3. `brg_territorio_classificacao`

**Grão:** um município por categoria territorial da IN MinC nº 10/2023.
**Universo:** as 13 categorias do art. 15, restrito às que tiverem fonte,
granularidade, limiar, versão e vigência homologados — decisão 5 da
[ADR 0006](adr/0006-silvers-salic-gate-a0-parcial.md).

**Para que serve:** classificar território vulnerabilizado para a Meta 4.

**Estado da fonte:** `transferegov.territorio_fcu_setores` foi ingerida e tem
33.272 linhas — grão de setor censitário, precisa colapsar para município. Era
a tabela que o `sources.yml` declarava e o banco não tinha.

**Desconhecidos:** município sem classificação é desconhecido, nunca `false` —
decisão 4 da ADR 0006. A cobertura tem de aparecer como medida.

**O que o modelo é, em uma frase:** uma tabela-ponte que diz, para cada
município, se ele é território vulnerabilizado e por qual categoria. Uma linha
por município e categoria, porque um município pode se enquadrar em mais de
uma — é o que impede a informação de virar coluna da dimensão.

**Bloqueio atual:** a classificação virá por **enriquecimento da base**,
decidido em 2026-09-04. O diagrama do P3 já desenha `base_territorio_vulneravel`
como base externa, com `flag_vulneravel` e `flag_periferia` — mais simples que
as 13 categorias abertas do art. 15.

---

## 4. `fct_execucao_municipal_rouanet`

**Grão:** um projeto por município de execução.
**Universo:** projetos com abrangência declarada em `sac__abrangencia`
(860.304 linhas, 860.304 ids distintos).

**Para que serve:** o numerador da Meta 4.

**Estado da ponte — reconferido em 04/09.** A ADR 0006 registra que a ponte
disponível recupera PRONAC para 45.022 linhas e **4.269 projetos**, cobertura
insuficiente para publicar. O `HANDOFF.md` levanta que `sac__projetos` poderia
levar a 155.113, e marca como não reconferido.

Medido na origem via Trino, `salic_sac.dbo.Projetos`:

```
277.187  linhas
277.187  com idPronac              (100%)
155.574  com idProjeto
155.574  idProjeto distintos       sem ambiguidade
155.574  com os dois na mesma linha
```

**Confirmado: 155.574**, contra os 4.269 de hoje. É 36 vezes mais, e os
`idProjeto` são distintos — nenhum sairia ambíguo pelo lado dessa chave, o que
casa com o desenho fail-closed do `map_chave_projeto_rouanet`.

**As 121.613 linhas sem `idProjeto` — investigado em 04/09.** Não são
propostas que nunca viraram projeto. O campo foi **introduzido em 2009**, e a
cobertura por ano mostra uma curva de adoção limpa:

| ano | projetos | com `idProjeto` | cobertura |
|---|---:|---:|---:|
| 1982–2008 | ~111.000 | 0 | **0%** |
| 2009 | 8.889 | 5.477 | 61,6% |
| 2010 | 12.860 | 9.521 | 74,0% |
| 2011 | 14.952 | 13.477 | 90,1% |
| 2012 | 10.374 | 9.465 | 91,2% |
| 2013 | 11.553 | 11.197 | 96,9% |
| 2014–2020 | — | — | 97,5% a 99,4% |
| 2021–2026 | — | — | **100%** |

**Consequência para o horizonte histórico.** A escolha deixa de ser preferência
e vira restrição:

- **antes de 2009** a ponte não existe, e nenhuma decisão de produto muda isso
  — o dado nunca foi registrado;
- **2009 a 2012** a cobertura vai de 62% a 91%, então qualquer indicador
  precisa declarar a lacuna;
- **2013 em diante** a cobertura é de 97% ou mais;
- **2021 em diante** é total.

Isso responde parte da decisão aberta da ADR 0006 sobre horizonte histórico da
Meta 5, e é o argumento mais concreto disponível para fixar o ano de corte.

**Bloqueio atual: só a regra de multicidade.** A fonte está resolvida — o
`sac__abrangencia` diz em quais municípios o projeto foi executado, com 860.304
linhas na bronze, e a ponte de PRONAC está medida acima.

O que falta é decisão, não dado: projeto executado em três municípios
**distribui** o valor entre eles ou **conta inteiro** em cada um? Somando os
três, o total dá o triplo do real; dividindo, some a noção de que o projeto
chegou naquela cidade. Decisão aberta da ADR 0006.

---

## 5. `fct_proponente_ano_rouanet`

**Grão:** um proponente por ano.
**Universo:** a definir com o conceito de primeiro acesso.

**Para que serve:** a Meta 5 — primeiro acesso a fomento.

**Estado da fonte:** `sac__preprojeto` foi ingerida com 449.580 linhas. O
`HANDOFF.md` registra que `fct_evento_acesso_rouanet` tem dois tipos de evento
em vez de três, porque `PreProjeto`/`Projetos` v2 não estavam ingeridas — e
avisa que derivar registro de `dtsituacao` seria errado, porque aquilo é a
última mudança de situação. **As duas chegaram**, então o terceiro tipo de
evento passa a ser construível.

`agentes__agentes` também tem `dtcadastro`, que o handoff aponta como o evento
que falta.

**Decidido em 2026-09-04, e isso destrava o modelo:**

**Primeiro acesso é o primeiro pagamento.** Não a captação, não a aprovação,
não o cadastro. A fonte é `tbApiComprovacoes`, com `vlPagamento` e
`aaAnoComprovacao`, ligada ao projeto por `idPronac`.

Vale fixar o que essa escolha significa: pagamento no SALIC é o projeto pagando
terceiros. Então o indicador mede o ano em que o projeto do proponente começou
a **executar**, não o ano em que ele captou. É uma definição defensável de
acesso a fomento — o recurso de fato se moveu — e é diferente de "conseguiu
recurso".

**O HMAC não se aplica.** O dado permanece no banco de origem, e a
disponibilização e a anonimização são feitas pelo **Apache Ranger**, no momento
do acesso. O modelo carrega o identificador real; quem não tem privilégio vê
mascarado.

Isso torna a classificação de PII mais importante, não menos: se as políticas
do Ranger forem dirigidas por classificação, coluna sem tag fica desprotegida
por omissão. Vale confirmar se o Ranger lê a classificação do OpenMetadata ou
se as políticas são escritas à mão.

**Bloqueio restante:** nenhum de decisão. O modelo pode ser escrito.

---

## 6. `dim_meta_alvo_rouanet` — movido para o gold

**Decidido em 2026-09-04: este modelo não pertence à camada silver.** A
especificação abaixo fica registrada para quem for construí-lo no gold.



**Grão:** uma meta por indicador por vigência.
**Universo:** as metas com base legal declarada.

**Para que serve:** dar o alvo contra o qual o gold compara o realizado.

**Colunas previstas:** `indicador`, `valor_alvo`, `unidade`, `base_legal`,
`vigencia_inicio`, `vigencia_fim`, `fonte`.

**Bloqueio atual:** é seed, não tem fonte no banco. Depende de alguém escrever
as metas com a base legal de cada uma.

**Ponto sensível:** as cotas de 25%, 10% e 5% da PNAB **não são metas
normativas da Rouanet** — decisão registrada no contexto da ADR 0006. Se
entrarem, entram como comparação não normativa, e isso precisa estar explícito
na própria dimensão para ninguém ler como obrigação legal.

---

## Ordem sugerida

1. **`dim_municipio_ibge`** — só depende de um `GRANT` e de um seed de 27
   linhas, e destrava outros dois.
2. **`dim_meta_alvo_rouanet`** — seed, independente de tudo.
3. **Achar a tabela de domínio de `stCorRaca`** — destrava o 2 e é o que
   transforma o achado de hoje em indicador.
4. **`fct_execucao_municipal_rouanet`** — depende do 1 e da decisão de
   multicidade.
5. **`dim_agente_perfil_rouanet_scd`** — depende do domínio e da decisão de
   produto sobre 2,9% de cobertura.
6. **`brg_territorio_classificacao`** — depende de homologar 13 categorias.

Os três primeiros não dependem do gate A0. Os três últimos, sim.
