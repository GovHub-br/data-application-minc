{% macro siconfi_tentativa_valida(itens, paginas, endpoint) -%}
    {#-
      Os itens de uma entidade do SICONFI que a silver usa, com a requisição e
      o run de onde vieram. Uma regra só para todas as silvers da pasta
      `siconfi/`, que só mudam o que tipam.

      A bronze guarda tudo o que o raw tem, de todos os runs (ADR 0014): a
      mesma requisição baixada de novo em outro run, e a mesma página carregada
      duas vezes (a carga caiu entre os itens e o registro em `paginas`). Daqui
      sai, nesta ordem:

      1. uma linha por página (`pagina_id`), a do registro mais recente;
      2. por `request_hash`, só as páginas da tentativa que vale: o run cuja
         página buscada por último (`fetched_at`) é a mais recente. Uma
         requisição com páginas de dois runs fica com as de um run só, mesmo
         que o outro tenha páginas a mais (paginação que mudou de tamanho, ou
         que falhou no meio): misturar daria itens repetidos ou de duas
         respostas diferentes. Empate em `fetched_at` vai para o maior `run_id`;
      3. uma linha por (`_pagina_id`, `_item`) dessas páginas.

      Nada é descartado pelo conteúdo do item.

      Devolve uma tabela derivada, para usar no `from` (`select ... from
      {{ siconfi_tentativa_valida(...) }}`).

      A forma da consulta não é estilo: é o que evita o nested loop entre itens
      e páginas, que cresce com itens × páginas. Duas escolhas:
      - as páginas válidas ficam numa CTE `materialized`, calculada uma vez por
        consulta. Sem ela, o Postgres refazia a subconsulta das páginas para
        cada item: a gold-base do Eixo 1 levou 72 minutos em 4 milhões de
        itens;
      - o join com os itens fica dentro da subconsulta da janela de
        (`_pagina_id`, `_item`). Os filtros de quem lê a silver (matriz, mês,
        anexo...) não descem abaixo da janela, porque não são colunas do
        `partition by`. O join é planejado sobre a bronze inteira, com as
        estatísticas dela, e sai hash join. Com o join acima da janela, esses
        filtros chegavam a ele, o lado dos itens era estimado em uma linha, e
        o nested loop comparava cada item com cada página: 21 minutos para ler
        a silver_rreo com 1,2 milhão de itens e 3.679 páginas.
      Filtrar as páginas antes da janela não muda o resultado dela: a página é
      parte do `partition by`.

      `itens` e `paginas` são os nomes das CTEs do modelo que leem, por
      `{{ ref() }}`, a bronze da entidade e bronze_siconfi_paginas. A `ref()`
      fica no modelo, e não aqui, para o verificar.py da skill
      arquitetura-lakehouse-minc ver de onde a silver lê. `endpoint` é como ele
      está no envelope (`msc_orcamentaria`, `rreo`, `entes`).
    -#}
    (
        with
            paginas_validas as materialized(
                select pagina_id, request_hash, run_id, fetched_at
                from
                    (
                        select
                            pagina_id,
                            request_hash,
                            run_id,
                            fetched_at,
                            _repeticao_pagina,
                            first_value(run_id) over (
                                partition by request_hash
                                order by ultima_busca_run desc nulls last, run_id desc
                            ) as run_valido
                        from
                            (
                                select
                                    pagina_id,
                                    request_hash,
                                    run_id,
                                    {{ bronze_timestamp("fetched_at") }} as fetched_at,
                                    max({{ bronze_timestamp("fetched_at") }}) over (
                                        partition by request_hash, run_id
                                    ) as ultima_busca_run,
                                    row_number() over (
                                        partition by pagina_id order by carregado_em desc
                                    ) as _repeticao_pagina
                                from {{ paginas }}
                                where endpoint = '{{ endpoint }}'
                            ) as pagina
                    ) as pagina_com_run
                where _repeticao_pagina = 1 and run_id = run_valido
            )

        select itens_validos.*
        from
            (
                select
                    bronze_itens.*,
                    paginas_validas.request_hash,
                    paginas_validas.run_id,
                    paginas_validas.fetched_at,
                    row_number() over (
                        partition by bronze_itens._pagina_id, bronze_itens._item
                        order by bronze_itens._item
                    ) as _repeticao_item
                from {{ itens }} as bronze_itens
                inner join
                    paginas_validas on bronze_itens._pagina_id = paginas_validas.pagina_id
            ) as itens_validos
        where itens_validos._repeticao_item = 1
    ) as tentativa_valida
{%- endmacro %}
