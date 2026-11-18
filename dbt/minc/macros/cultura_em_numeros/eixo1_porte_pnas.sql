{% macro eixo1_porte_pnas(populacao) -%}
    {#-
      Porte do município pelas faixas da PNAS (Política Nacional de Assistência
      Social), as mesmas da MUNIC_FINAL.xlsx que gasto_municipal.R lê: Pequeno
      Porte I até 20.000 habitantes, Pequeno Porte II até 50.000, Médio Porte
      até 100.000, Grande Porte até 900.000 e Metrópole acima disso. População
      nula dá porte nulo.

      É regra do Eixo 1 (quem é de que porte): só as golds
      eixo1_orcamento_municipal_* usam, sobre a população do Censo 2022.
    -#}
    case
        when {{ populacao }} <= 20000
        then 'Pequeno Porte I'
        when {{ populacao }} <= 50000
        then 'Pequeno Porte II'
        when {{ populacao }} <= 100000
        then 'Médio Porte'
        when {{ populacao }} <= 900000
        then 'Grande Porte'
        when {{ populacao }} > 900000
        then 'Metrópole'
    end
{%- endmacro %}
