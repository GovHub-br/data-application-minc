{% macro salt_documento() %}
    {#-
  Devolve o salt secreto, validando-o. Fica ao lado de normaliza_documento
  porque é a mesma família de macros: aquelas preparam o documento, estas o
  substituem.

  A validação é o ponto do macro. Um salt vazio produziria sha256(cpf) puro, e
  hash puro de CPF não protege nada: o espaço é de ~10⁹ documentos válidos, e a
  tabela inteira `sha256(cpf) -> cpf` se constrói num laptop em minutos. Por
  isso falhamos alto, em tempo de compilação, em vez de gerar em silêncio uma
  coluna que só parece anonimizada.

  A restrição de alfabeto tem um segundo motivo além do higiênico: o salt é
  interpolado direto no SQL compilado, então uma aspa dentro dele quebraria a
  query. Gere com `openssl rand -base64 48`.
-#}
    {%- set salt = var("salt_documento", "") | string -%}

    {%- if salt | length < 32 -%}
        {{
            exceptions.raise_compiler_error(
                "MINC_SALT_DOCUMENTO ausente ou com menos de 32 caracteres. "
                ~ "Sem salt secreto, o hash do CPF é revertido por força bruta em "
                ~ "minutos — a coluna pareceria anonimizada sem estar. "
                ~ "Gere com `openssl rand -base64 48` e exporte antes de rodar o dbt."
            )
        }}
    {%- endif -%}

    {%- if modules.re.search("[^A-Za-z0-9+/=_-]", salt) -%}
        {{
            exceptions.raise_compiler_error(
                "MINC_SALT_DOCUMENTO tem caractere fora de [A-Za-z0-9+/=_-]. "
                ~ "O salt é interpolado no SQL compilado; aspas e afins quebrariam "
                ~ "a query. Gere com `openssl rand -base64 48`."
            )
        }}
    {%- endif -%}

    {{- return(salt) -}}
{% endmacro %}


{% macro documento_canonico(col, tipo_pessoa=none) %}
    {#-
  Forma canônica do documento ANTES do hash: só dígitos, com LPAD para 11 (CPF)
  ou 14 (CNPJ).

  Tem que ser idêntica em todas as fontes. Enquanto o documento circulava em
  claro, os JOINs consertavam a diferença na hora com REGEXP_REPLACE inline;
  sob hash isso deixa de existir — se uma fonte hashear "NNN.NNN.NNN-NN" e
  outra "NNNNNNNNNNN", o mesmo CPF vira dois hashes diferentes e o JOIN passa a
  devolver zero linha, sem erro e sem aviso.

  `tipo_pessoa` é a coluna em que a fonte declara PF/PJ ('1'/'2' no BB Ágil).
  Quando existe ela decide o alvo do LPAD, e é mais confiável que o
  comprimento: documento que chegou sem os zeros à esquerda engana a heurística
  de tamanho. Sem ela, caímos no comprimento — o mesmo critério que
  identificadores_contemplados já usava.

  NULL quando não sobra dígito nenhum.

  ATENÇÃO: não passe por aqui documento mascarado ('***.NNN.NNN-**'). Ele tem 6
  dígitos e sairia daqui como '00000NNNNNN', um CPF de aparência legítima que
  colidiria com o CPF real que começa com cinco zeros. Para esses use
  documento_anonimizado() e trate à parte — ver os modelos de planilha.
-#}
    {%- set digitos -%}
        nullif(regexp_replace(coalesce({{ col }}, ''), '[^0-9]', '', 'g'), '')
    {%- endset -%}

    {%- if tipo_pessoa is none %}
        case
            when {{ digitos }} is null
            then null
            when length({{ digitos }}) <= 11
            then lpad({{ digitos }}, 11, '0')
            else lpad({{ digitos }}, 14, '0')
        end
    {%- else %}
        case
            when {{ digitos }} is null
            then null
            when {{ tipo_pessoa }} = '1'
            then lpad({{ digitos }}, 11, '0')
            when {{ tipo_pessoa }} = '2'
            then lpad({{ digitos }}, 14, '0')
        end
    {%- endif %}
{% endmacro %}


{% macro hash_documento(col) %}
    {#-
  Pseudônimo estável do documento: sha256(salt || valor), em hex (64 chars).

  sha256() é nativo do Postgres desde a 11, então não dependemos de pgcrypto —
  o que importa aqui na prática, porque criar extensão exigiria mexer no
  init.sh, que só roda em volume vazio e portanto não pegaria num banco já
  inicializado.

  Determinístico de propósito: é isso que mantém de pé todo JOIN entre fontes
  de que a Meta 5 depende. E é também o motivo de o salt ser permanente —
  trocá-lo reescreve todos os pseudônimos e desfaz qualquer comparação com dado
  já materializado.

  Espera receber o valor JÁ canônico (ver documento_canonico).
-#}
    case
        when ({{ col }}) is null
        then null
        else
            encode(
                sha256(convert_to('{{ salt_documento() }}' || ({{ col }}), 'UTF8')), 'hex'
            )
    end
{% endmacro %}


{% macro miolo_cpf(col) %}
    {#-
  Os 6 dígitos centrais do CPF (posições 4-9) — os mesmos que a base de
  proponentes da LPG deixa visíveis ao mascarar ('***.NNN.NNN-**').

  Existe só para sustentar o match parcial de primeiro_acesso_contemplados:
  hash não preserva substring, então o miolo precisa ser recortado e hasheado
  separadamente, dos dois lados do JOIN, para aquele fallback continuar
  existindo. NULL para CNPJ, que não é mascarado na origem.

  Espera receber o valor já canônico.
-#}
    case when length({{ col }}) = 11 then substring({{ col }} from 4 for 6) end
{% endmacro %}
