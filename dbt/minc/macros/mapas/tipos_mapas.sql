{#-
  Casts da silver do Mapas Culturais que as macros bronze_* não cobrem.

  A bronze do Mapas (mapas.bronze_<tabela>) é uma cópia fiel: tudo chega como
  texto, e é a silver que tipa (ver o padrão de camadas do Lakehouse). Para os
  tipos comuns use as macros bronze_texto, bronze_inteiro, bronze_numerico,
  bronze_timestamp e bronze_booleano; as duas daqui são para os dois tipos que
  o PostgreSQL do Mapas tem e que elas não entendem: `point` e `json`.

  A regra é a mesma das outras: valor que não casa vira NULL em vez de derrubar
  o modelo. Ausente fica nulo.
-#}
{% macro mapas_ponto(col, eixo) -%}
    {#- `point` do PostgreSQL chega na bronze como texto "(x,y)", por exemplo
        "(-47.91,-15.79)". x é a longitude e y é a latitude: conferido nos
        agregados do banco real, onde x vai de -131 a 14 (faixa que não cabe numa
        latitude) e y vai de -43 a 19.

        eixo = 1 devolve a longitude, eixo = 2 a latitude.

        Vira NULL, em vez de valor, o que não pode ser coordenada:
        * fora de [-180, 180] na longitude ou de [-90, 90] na latitude;
        * exatamente "(0,0)", que é o ponto que o cadastro grava quando não há
          localização — no oceano, a centenas de km da costa brasileira.
        Coordenada válida mas fora do Brasil NÃO é tratada: é erro de cadastro
        que a silver não tem como corrigir, e fica documentado.

        Os dois CASE são aninhados de propósito: o Postgres não garante a ordem
        de avaliação de um AND, e o cast para numeric rodaria também nas linhas
        que não casam com o padrão. O expoente é limitado a 3 dígitos para o
        cast não estourar. -#}
    case
        when
            trim({{ col }})
            ~ '^\(-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]{1,3})?,-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]{1,3})?\)$'
        then
            case
                when
                    trim({{ col }}) <> '(0,0)'
                    and split_part(
                        trim(both '()' from trim({{ col }})), ',', {{ eixo }}
                    )::numeric
                    between {{ -180 if eixo == 1 else -90 }}
                    and {{ 180 if eixo == 1 else 90 }}
                then
                    split_part(
                        trim(both '()' from trim({{ col }})), ',', {{ eixo }}
                    )::numeric
            end
    end
{%- endmacro %}


{% macro mapas_json(col) -%}
    {#- A bronze guarda json/jsonb como o texto do JSON (json_format no Trino).
        Texto vazio e o literal 'null' são ausência de valor. O resto vira
        jsonb: a coluna de origem é json/jsonb, então o Postgres já validou o
        texto, e o cast não tem como falhar. -#}
    case
        when nullif(trim({{ col }}), '') is null
        then null
        when trim({{ col }}) = 'null'
        then null
        else trim({{ col }})::jsonb
    end
{%- endmacro %}
