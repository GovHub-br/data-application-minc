{#-
  A regra "sim ou não" dos scripts do Eixo 1 do Cultura em Números (o fix_sim_nao
  do MUNIC.R): a resposta que começa por "sim", ou é "1" ou "s", vira 'Sim';
  qualquer outra resposta preenchida vira 'Não'.

  EXCEÇÃO À REGRA TRANSVERSAL 1 DO ADR 0010, decidida para as golds baterem com a
  publicação: '-' (não se aplica), 'Recusa', 'Não informou' e 'Não disponível'
  viram 'Não', como no R, e entram no denominador como "não tem". Quantas
  respostas caem nela, por edição e coluna, está no schema.yml de cada gold-base
  do Eixo 1. Resposta ausente (NULL) continua NULL; o R a transformava em 'Não'.

  Diferença do R que não muda número: o R casa "sim" em qualquer posição. Aqui
  ele tem de abrir a resposta. Em 09/11/2026, nas 7.627 colunas da MUNIC e da
  ESTADIC em seeds, "sim" fora do começo só aparece em nome de lugar, endereço
  e e-mail, nenhuma coluna que passa por aqui.

  `~*` e não lower(): o lower() do Postgres depende do locale do banco para letra
  acentuada, e o padrão aqui é todo ASCII.
-#}
{% macro eixo1_sim_nao(col) -%}
    case
        when {{ col }} is null
        then null
        when {{ col }} ~* '^sim|^1$|^s$'
        then 'Sim'
        else 'Não'
    end
{%- endmacro %}

{#-
  Só 'Sim' e 'Não' exatos; qualquer outra resposta vira NULL. É o que o MUNIC.R
  faz com os equipamentos de 2006, 2014 e 2018: guarda a resposta crua e o
  gráfico 11 conta só 'Sim' e 'Não'. Sem a exceção da macro acima.
-#}
{% macro eixo1_sim_nao_exato(col) -%}
    case when {{ col }} in ('Sim', 'Não') then {{ col }} end
{%- endmacro %}
