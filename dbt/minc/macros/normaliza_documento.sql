{% macro normaliza_documento(col) %}
    {#-
  Normaliza CPF/CNPJ para chave de junção: mantém só dígitos, vazio -> NULL.
  Sempre retorna text. CPF anonimizado do PNAB (com '*') vira NULL de dígitos
  aqui; a flag de "não casável" é derivada à parte via `documento_anonimizado`.
-#}
    nullif(regexp_replace(coalesce({{ col }}, ''), '[^0-9]', '', 'g'), '')
{% endmacro %}


{% macro documento_anonimizado(col) %}
    {#- true quando o documento vem mascarado (contém '*'), logo não casável. -#}
    (coalesce({{ col }}, '') like '%*%')
{% endmacro %}


{% macro documento_valido(col) -%}
    {#- Digito verificador de CPF (11 digitos) ou CNPJ (14) pelo modulo 11 da
    Receita. Recebe o documento ja normalizado (so digitos, normaliza_documento).
    Nulo continua nulo; tamanho fora de 11/14 e sequencia de um digito so
    (00000000000) sao invalidos. shortcut: so CNPJ numerico; o CNPJ
    alfanumerico da Receita (a partir de julho de 2026) vira invalido aqui e
    sai do normaliza_documento sem as letras, revisar quando aparecer. -#}
    case
        when {{ col }} is null
        then null
        when {{ col }} ~ '^(\d)\1*$'
        then false
        when length({{ col }}) = 11
        then
            {{ dv_modulo_11(col, [10, 9, 8, 7, 6, 5, 4, 3, 2]) }}
            = substr({{ col }}, 10, 1)::integer
            and {{ dv_modulo_11(col, [11, 10, 9, 8, 7, 6, 5, 4, 3, 2]) }}
            = substr({{ col }}, 11, 1)::integer
        when length({{ col }}) = 14
        then
            {{ dv_modulo_11(col, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]) }}
            = substr({{ col }}, 13, 1)::integer
            and {{ dv_modulo_11(col, [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]) }}
            = substr({{ col }}, 14, 1)::integer
        else false
    end
{%- endmacro %}


{% macro dv_modulo_11(col, pesos) -%}
    {#- Um digito verificador: soma dos primeiros len(pesos) digitos vezes os pesos,
    resto por 11; resto 0 ou 1 da digito 0, os outros 11 menos o resto. -#}
    mod(
        mod(
            11 - mod(
                {%- for peso in pesos %}
                    substr({{ col }}, {{ loop.index }}, 1)::integer * {{ peso }}
                    {%- if not loop.last %} +{% endif %}
                {%- endfor %},
                11
            ),
            11
        ),
        10
    )
{%- endmacro %}
