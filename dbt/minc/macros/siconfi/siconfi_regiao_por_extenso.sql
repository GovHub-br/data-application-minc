{% macro siconfi_regiao_por_extenso(sigla) -%}
    {#-
      A sigla de região do cadastro de entes do SICONFI ('NO', 'NE', 'CO', 'SE',
      'SU') por extenso, como as figuras do Cultura em Números escrevem. Outra
      sigla (a União tem 'BR') ou nulo dá nulo.
    -#}
    case
        {{ sigla }}
        when 'NO'
        then 'Norte'
        when 'NE'
        then 'Nordeste'
        when 'CO'
        then 'Centro-Oeste'
        when 'SE'
        then 'Sudeste'
        when 'SU'
        then 'Sul'
    end
{%- endmacro %}
