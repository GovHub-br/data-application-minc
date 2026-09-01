-- Bronze SALIC — tabelas__menus.
-- Origem: salic_bronze.tabelas__menus, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 5 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("men_codigo") }} as men_codigo,
    {{ bronze_inteiro("men_sistema") }} as men_sistema,
    {{ bronze_texto("men_modulo") }} as men_modulo,
    {{ bronze_inteiro("men_menu") }} as men_menu,
    {{ bronze_inteiro("men_opcao") }} as men_opcao,
    {{ bronze_texto("men_nome") }} as men_nome,
    {{ bronze_texto("men_exibicao") }} as men_exibicao,
    {{ bronze_inteiro("men_status") }} as men_status,
    {{ bronze_texto("men_seguranca") }} as men_seguranca,
    {{ bronze_texto("men_controle") }} as men_controle,
    {{ bronze_texto("men_aplicacao") }} as men_aplicacao,
    _fatia
from {{ source("bronze_tabelas", "tabelas__menus") }}
