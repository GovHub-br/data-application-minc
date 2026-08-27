-- Guarda-costas da pseudonimização: varre TODA coluna de texto do schema de
-- agentes procurando valor que ainda pareça CPF/CNPJ (11 ou 14 dígitos) ou
-- documento mascarado (contendo '*'). Devolver linha significa que algum
-- modelo ficou para trás — hoje, ou num modelo novo escrito daqui a seis meses.
--
-- É deliberadamente genérico, e não uma lista de colunas a proteger: uma lista
-- precisaria ser lembrada a cada modelo novo, e é exatamente o que ninguém
-- lembra. Varrer o catálogo é o que faz este teste continuar valendo sozinho.
--
-- COLUNAS DISPENSADAS abaixo são as que legitimamente guardam cadeia de
-- dígitos e não são documento de pessoa. A lista falha para o lado barulhento
-- de propósito: coluna numérica nova acusa uma vez e alguém a acrescenta aqui,
-- em vez de um documento em claro passar em silêncio.
{% set colunas_dispensadas = [
    "id",
    "id_plano_acao",
    "pronac",
    "no_salic",
    "programa_curto",
] %}

{% set schema_alvo = ref("primeiro_acesso_agentes").schema %}

{% set consulta_colunas %}
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = '{{ schema_alvo }}'
      AND data_type IN ('text', 'character varying', 'character')
      AND column_name NOT IN (
          {%- for c in colunas_dispensadas %}
          '{{ c }}'{% if not loop.last %},{% endif %}
          {%- endfor %}
      )
    ORDER BY table_name, column_name
{% endset %}

{% set colunas = [] %}
{% if execute %}
    {% for linha in run_query(consulta_colunas).rows %}
        {% do colunas.append((linha[0], linha[1])) %}
    {% endfor %}
{% endif %}

{% if colunas | length == 0 %}

    -- Schema ainda não materializado: nada a checar, e o teste passa em vez de
    -- quebrar a suíte inteira num banco novo.
    select null::text as tabela, null::text as coluna where false

{% else %}
    {% for tabela, coluna in colunas %}
        (
            select
                '{{ tabela }}'::text as tabela,
                -- o valor NÃO é devolvido: ele seria justamente o dado que estamos
                -- tentando não vazar. O teste precisa dizer ONDE está, não o quê.
                '{{ coluna }}'::text as coluna
            from {{ schema_alvo }}."{{ tabela }}"
            where
                "{{ coluna }}" ~ '^[0-9]{11}$'
                or "{{ coluna }}" ~ '^[0-9]{14}$'
                or "{{ coluna }}" like '%*%'
            limit 1
        )
        {%- if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
{% endif %}
