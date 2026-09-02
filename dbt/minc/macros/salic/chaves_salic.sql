{#-
  Chaves do SALIC, para a camada silver da Rouanet.

  O PRONAC e a chave de negocio do projeto, e ele chega de tres formas
  diferentes na bronze v2:

    - `anoprojeto` (char 2) + `sequencial` (varchar 5), nas tabelas
      transacionais -- `sac__captacao`, `sac__aprovacao`, `sac__enquadramento`;
    - `pronac`, texto de 7 posicoes, na maioria das views;
    - `pronac`, inteiro, em algumas views (ex.:
      `sac__vwpagamentodefornecedordoprojetoporitemdetalhado`) -- e ai o zero a
      esquerda ja se perdeu na origem.

  Juntar as tres sem normalizar produz join vazio, em silencio, para todo
  projeto cujo sequencial comeca com zero. Estas macros existem para que essa
  regra viva num lugar so, como as `bronze_*` fazem para os casts.

  Mesma politica de guarda das macros da bronze: valor que nao casa com o
  padrao vira NULL, nao erro. Chave suja e ausencia de chave, nao excecao.
-#}
{% macro pronac_digitos(col) -%}
    nullif(regexp_replace(coalesce({{ col }}::text, ''), '[^0-9]', '', 'g'), '')
{%- endmacro %}


{% macro pronac_normalizado(col) -%}
    {#- 7 posicoes, zeros a esquerda restaurados. Mais de 7 digitos, ou so
        zeros, nao identifica projeto nenhum: vira NULL. -#}
    case
        when
            {{ pronac_digitos(col) }} ~ '^[0-9]{1,7}$'
            and {{ pronac_digitos(col) }} !~ '^0+$'
        then lpad({{ pronac_digitos(col) }}, 7, '0')
    end
{%- endmacro %}


{% macro pronac_de_ano_sequencial(ano, sequencial) -%}
    {#- O `lpad` do Postgres TRUNCA quando a string ja e maior que o tamanho
        pedido: um ano gravado como '2023' viraria '20' sem avisar. Por isso o
        guarda de regex vem antes, e nao depois. -#}
    case
        when trim({{ ano }}) ~ '^[0-9]{1,2}$' and trim({{ sequencial }}) ~ '^[0-9]{1,5}$'
        then lpad(trim({{ ano }}), 2, '0') || lpad(trim({{ sequencial }}), 5, '0')
    end
{%- endmacro %}


{% macro ano_do_pronac(col) -%}
    {#- As duas primeiras posicoes do PRONAC sao o ano de dois digitos. A
        Rouanet e de 1991, entao 91..99 e seculo XX e 00..90 e seculo XXI --
        regra correta ate 2091, quando a ambiguidade volta. -#}
    case
        when substring({{ col }} from 1 for 2) ~ '^[0-9]{2}$'
        then
            case
                when substring({{ col }} from 1 for 2)::integer >= 91
                then 1900 + substring({{ col }} from 1 for 2)::integer
                else 2000 + substring({{ col }} from 1 for 2)::integer
            end
    end
{%- endmacro %}


{% macro tipo_pessoa_documento(col) -%}
    {#- Derivacao NAO identificante: le so o comprimento do documento, para que
        a silver possa dizer "pessoa fisica" ou "juridica" sem carregar o
        CPF/CNPJ junto. Comprimento fora de 11/14 vira NULL -- documento
        invalido nao vira uma terceira categoria. -#}
    case length({{ normaliza_documento(col) }}) when 11 then 'PF' when 14 then 'PJ' end
{%- endmacro %}
