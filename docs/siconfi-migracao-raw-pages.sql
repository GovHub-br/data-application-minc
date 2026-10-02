-- SICONFI: raw_pages única -> uma tabela de páginas por endpoint.
--
-- Rodar UMA vez, antes de publicar o código novo de plugins/siconfi_storage.py:
--   1. pausar a siconfi_ingestion_dag e esperar as tasks em execução terminarem;
--   2. rodar este script;
--   3. publicar o código e despausar a DAG.
--
-- Não apaga nada. raw_pages e msc_orcamentaria_items continuam no banco até a
-- bronze do dbt estar validada; depois disso:
--   DROP TABLE siconfi_bronze.msc_orcamentaria_items;  -- ~45 GB
--   DROP TABLE siconfi_bronze.raw_pages;
-- (nesta ordem: a FK de msc_orcamentaria_items ainda aponta para raw_pages).
--
-- Seguro de repetir: copia só as páginas com id maior que o já copiado.

DO $$
DECLARE
    endpoint_name text;
    pages_table   text;
    items_table   text;
    last_copied   bigint;
    total_origem  bigint;
    total_destino bigint;
BEGIN
    FOREACH endpoint_name IN ARRAY ARRAY[
        'anexos-relatorios', 'entes', 'extrato_entregas', 'rreo', 'rgf', 'dca',
        'msc_patrimonial', 'msc_orcamentaria', 'msc_controle'
    ]
    LOOP
        pages_table := 'raw_pages_' || replace(endpoint_name, '-', '_');
        items_table := replace(endpoint_name, '-', '_') || '_items';

        -- Mesma definição de _PAGES_DDL em plugins/siconfi_storage.py.
        EXECUTE format($ddl$
            CREATE TABLE IF NOT EXISTS siconfi_bronze.%I (
                raw_page_id BIGSERIAL PRIMARY KEY,
                endpoint TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                request_params JSONB NOT NULL,
                page_offset INTEGER NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL,
                run_id TEXT NOT NULL,
                response_headers JSONB NOT NULL,
                payload JSONB NOT NULL,
                item_count INTEGER NOT NULL
            )$ddl$, pages_table);

        -- Os ids antigos são preservados: os itens já gravados apontam para eles.
        EXECUTE format(
            'SELECT coalesce(max(raw_page_id), 0) FROM siconfi_bronze.%I', pages_table
        ) INTO last_copied;
        EXECUTE format($copy$
            INSERT INTO siconfi_bronze.%I
                (raw_page_id, endpoint, request_hash, request_params, page_offset,
                 fetched_at, run_id, response_headers, payload, item_count)
            SELECT raw_page_id, endpoint, request_hash, request_params, page_offset,
                   fetched_at, run_id, response_headers, payload, item_count
            FROM siconfi_bronze.raw_pages
            WHERE endpoint = %L AND raw_page_id > %s
            ORDER BY raw_page_id$copy$, pages_table, endpoint_name, last_copied);

        -- Confere a cópia; se divergir, o script inteiro é desfeito.
        EXECUTE 'SELECT count(*) FROM siconfi_bronze.raw_pages WHERE endpoint = $1'
            INTO total_origem USING endpoint_name;
        EXECUTE format('SELECT count(*) FROM siconfi_bronze.%I', pages_table)
            INTO total_destino;
        IF total_origem <> total_destino THEN
            RAISE EXCEPTION '% : % páginas em raw_pages, % em %',
                endpoint_name, total_origem, total_destino, pages_table;
        END IF;
        RAISE NOTICE '% : % páginas copiadas para %',
            endpoint_name, total_destino, pages_table;

        -- A sequence recomeça depois do maior id copiado.
        EXECUTE format(
            'SELECT setval(pg_get_serial_sequence(%L, %L), '
            '(SELECT coalesce(max(raw_page_id), 0) + 1 FROM siconfi_bronze.%I), false)',
            'siconfi_bronze.' || pages_table, 'raw_page_id', pages_table
        );

        -- A tabela de itens passa a apontar para a página do próprio endpoint.
        -- NOT VALID evita varrer dezenas de milhões de linhas; o que já existe
        -- foi copiado acima, e as novas linhas são checadas.
        -- msc_orcamentaria_items fica como está: não recebe mais linhas.
        IF endpoint_name <> 'msc_orcamentaria'
           AND to_regclass('siconfi_bronze.' || items_table) IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE siconfi_bronze.%I DROP CONSTRAINT IF EXISTS %I',
                items_table, items_table || '_raw_page_id_fkey'
            );
            EXECUTE format(
                'ALTER TABLE siconfi_bronze.%I ADD CONSTRAINT %I '
                'FOREIGN KEY (raw_page_id) REFERENCES siconfi_bronze.%I (raw_page_id) '
                'NOT VALID',
                items_table, items_table || '_raw_page_id_fkey', pages_table
            );
        END IF;
    END LOOP;
END
$$;
