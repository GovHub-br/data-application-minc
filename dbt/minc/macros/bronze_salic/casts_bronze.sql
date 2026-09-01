{#-
  Casts da camada bronze do SALIC.

  Tudo em `salic_bronze` chega como texto: a ingestão via Trino converte cada
  coluna para TEXT (plugins/trino_bronze.py, cast_to_text — ver ADR 0005).
  Tipar é o trabalho desta camada, e os 571 modelos da bronze usam só estas
  macros, para que a regra do cast viva num lugar só.

  A regra comum a todas: **o cast é guardado por regex**. Valor que não casa
  com o padrão vira NULL em vez de derrubar o modelo inteiro em execução. Num
  raw de 157 milhões de linhas sem nenhuma constraint, um único valor sujo
  numa coluna quebraria o run — e a bronze existe justamente para absorver
  essa sujeira, não para propagá-la.

  Todas aplicam `trim` antes: a origem grava padding, e ' 2025-04-30 23:27:44 '
  aparece de verdade no dado.
-#}

{% macro bronze_texto(col) -%}
    {#- String vazia e string só de espaço viram NULL: no SQL Server elas são
        ausência de valor, e mantê-las como '' obrigaria todo consumidor a
        testar as duas coisas. -#}
    nullif(trim({{ col }}), '')
{%- endmacro %}


{% macro bronze_inteiro(col, tipo='integer') -%}
    case when trim({{ col }}) ~ '^-?[0-9]+$' then trim({{ col }})::{{ tipo }} end
{%- endmacro %}


{% macro bronze_numerico(col) -%}
    {#- Só ponto decimal: o levantamento das 571 tabelas não achou uma única
        coluna com vírgula decimal no salic_bronze. Se aparecer, use a macro
        parse_valor, que normaliza o padrão brasileiro. -#}
    case
        when trim({{ col }}) ~ '^-?[0-9]+(\.[0-9]+)?$' then trim({{ col }})::numeric
    end
{%- endmacro %}


{% macro bronze_timestamp(col) -%}
    {#- Âncora só no prefixo ISO da data: a origem mistura '2014-02-03 16:16:33.540',
        '2007-05-03 12:13:00', '2026-04-30' e até 7 casas de fração, e o Postgres
        aceita todos. Exigir o formato completo descartaria dado bom. -#}
    case
        when trim({{ col }}) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            then trim({{ col }})::timestamp
    end
{%- endmacro %}


{% macro bronze_data(col) -%}
    case
        when trim({{ col }}) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then trim({{ col }})::date
    end
{%- endmacro %}


{% macro bronze_booleano(col) -%}
    {#- O SQL Server grava bit(1) como '0'/'1', mas a passagem pelo Trino também
        produz 'true'/'false' e 't'/'f' dependendo da tabela. Qualquer outro
        valor vira NULL — inclusive '2', que aparece em coluna declarada bit e
        não tem leitura booleana honesta. -#}
    case lower(trim({{ col }}))
        when '1' then true
        when 'true' then true
        when 't' then true
        when '0' then false
        when 'false' then false
        when 'f' then false
    end
{%- endmacro %}
