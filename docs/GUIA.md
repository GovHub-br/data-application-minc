# `docs/`

Documentação escrita à mão, que não é gerada de nada.

| Caminho | O que é |
|---|---|
| [`adr/`](adr/) | registro das decisões de arquitetura — por que cada escolha foi feita e o que foi descartado |
| [`documentos/`](documentos/) | todos os documentos produzidos para o TED, com o gerador dos que são apurados do código |
| [`entregas/`](entregas/) | os documentos de entrega das Metas 02 a 04, gerados a partir do código |
| [`fase1-fundamentos/`](fase1-fundamentos/) | os quatro artefatos da Fase 1 — Fundamentos de Dados |
| [`relatorio-parcial-03/`](relatorio-parcial-03/) | anexos do 3º Relatório Parcial que não são gerados deste repositório |
| `index.html` | página estática legada, publicada manualmente na branch `gh-pages` |

## O que **não** está aqui

O **site de documentação dos dados** fica em [`../docs-pages/`](../docs-pages/).
Aquilo é gerado: os fatos são lidos do repositório a cada coleta, e só a
narrativa é escrita à mão. Nada em `docs/` alimenta aquele site.

A distinção que vale guardar: aqui mora o que só existe porque alguém escreveu;
lá mora o que se atualiza sozinho a partir do código.

## Sobre o `index.html`

É a página de metas publicada à mão na branch `gh-pages` em 30/07/2026, e é o
que está no ar hoje no endereço do projeto. Ela **sai do ar** no momento em que o
GitHub Pages for trocado do modo "branch" para o modo "GitHub Actions", que é o
que a publicação automática de `docs-pages/` exige. Ver
[ADR 0004](adr/0004-revisao-semanal-apura-antes-de-narrar.md) e o
[guia do docs-pages](../docs-pages/GUIA.md).
