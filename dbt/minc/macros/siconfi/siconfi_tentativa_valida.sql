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

      As páginas válidas saem de uma passada só sobre `paginas`, por funções de
      janela, numa CTE `materialized`. Não é estilo: os filtros de quem lê a
      silver (matriz, mês, classe...) descem até os itens e o Postgres passa a
      estimar uma linha só; com isso escolhe nested loop e, se o lado das
      páginas for uma subconsulta, a refaz inteira para cada item. Foi o que
      levou a gold-base do Eixo 1 a 72 minutos em 4 milhões de itens. A CTE
      materializada é calculada uma vez por consulta, e reler o resultado dela
      custa uma varredura de poucas centenas de linhas.

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

        select
            itens_unicos.*,
            paginas_validas.request_hash,
            paginas_validas.run_id,
            paginas_validas.fetched_at
        from
            (
                select itens_bronze.*
                from
                    (
                        select
                            bronze_itens.*,
                            row_number() over (
                                partition by bronze_itens._pagina_id, bronze_itens._item
                                order by bronze_itens._item
                            ) as _repeticao_item
                        from {{ itens }} as bronze_itens
                    ) as itens_bronze
                where itens_bronze._repeticao_item = 1
            ) as itens_unicos
        inner join paginas_validas on itens_unicos._pagina_id = paginas_validas.pagina_id
    ) as tentativa_valida
{%- endmacro %}
