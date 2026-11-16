-- A macro siconfi_tentativa_valida, que as três silvers do SICONFI usam
-- (silver_entes, silver_rreo e silver_msc_orcamentaria), num caso pequeno por
-- regra. Devolve as linhas que sobram ou faltam contra o esperado.
-- • requisição ra, run vencedor: o r2 buscou por último e fica, mesmo com uma
-- página contra duas do r1;
-- • rb, empate em fetched_at: vai para o maior run_id;
-- • rc, página repetida: vale o registro de paginas mais recente
-- (carregado_em; o fetched_at diferente só mostra qual ficou). O item repetido
-- igual sai uma vez, e o repetido com conteúdo diferente fica com a versão de
-- menor md5 da linha em texto (c1-1-y), a mesma em todo run, mesmo vindo
-- depois da outra;
-- • pd1, de outro endpoint, e pe1, item sem página registrada, não entram.
with
    -- As duas CTEs só são lidas dentro da macro, que o lint não expande.
    fx_paginas as (  -- noqa: ST03
        select *
        from
            (
                values
                    ('pa1', 'ra', 'r1', '2026-01-01 10:00', '2026-01-05', 'entes'),
                    ('pa2', 'ra', 'r1', '2026-01-01 10:05', '2026-01-05', 'entes'),
                    ('pa3', 'ra', 'r2', '2026-01-02 09:00', '2026-01-05', 'entes'),
                    ('pb1', 'rb', 'r1', '2026-01-03 08:00', '2026-01-05', 'entes'),
                    ('pb2', 'rb', 'r2', '2026-01-03 08:00', '2026-01-05', 'entes'),
                    ('pc1', 'rc', 'r1', '2026-01-04 10:00', '2026-01-05', 'entes'),
                    ('pc1', 'rc', 'r1', '2026-01-04 10:30', '2026-01-06', 'entes'),
                    ('pd1', 'rd', 'r1', '2026-01-04 10:00', '2026-01-05', 'rreo')
            ) as t(pagina_id, request_hash, run_id, fetched_at, carregado_em, endpoint)  -- noqa: LT01
    ),

    fx_itens as (  -- noqa: ST03
        select *
        from
            (
                values
                    ('pa1', '0', 'a1-0'),
                    ('pa1', '1', 'a1-1'),
                    ('pa2', '0', 'a2-0'),
                    ('pa3', '0', 'a3-0'),
                    ('pb1', '0', 'b1-0'),
                    ('pb2', '0', 'b2-0'),
                    ('pc1', '0', 'c1-0'),
                    ('pc1', '0', 'c1-0'),
                    ('pc1', '1', 'c1-1-x'),
                    ('pc1', '1', 'c1-1-y'),
                    ('pd1', '0', 'd1-0'),
                    ('pe1', '0', 'e1-0')
            ) as t(_pagina_id, _item, valor)  -- noqa: LT01
    ),

    obtido as (
        select _pagina_id, _item, valor, request_hash, run_id, fetched_at
        from {{ siconfi_tentativa_valida("fx_itens", "fx_paginas", "entes") }}
    ),

    esperado as (
        select
            _pagina_id,
            _item,
            valor,
            request_hash,
            run_id,
            fetched_at::timestamp as fetched_at
        from
            (
                values
                    ('pa3', '0', 'a3-0', 'ra', 'r2', '2026-01-02 09:00'),
                    ('pb2', '0', 'b2-0', 'rb', 'r2', '2026-01-03 08:00'),
                    ('pc1', '0', 'c1-0', 'rc', 'r1', '2026-01-04 10:30'),
                    ('pc1', '1', 'c1-1-y', 'rc', 'r1', '2026-01-04 10:30')
            ) as t(_pagina_id, _item, valor, request_hash, run_id, fetched_at)  -- noqa: LT01
    )

select 'sobra' as erro, sobra.*
from
    (
        select *
        from obtido
        except all
        select *
        from esperado
    ) as sobra

union all

select 'falta' as erro, falta.*
from
    (
        select *  -- noqa: AM04
        from esperado
        except all
        select *
        from obtido
    ) as falta
