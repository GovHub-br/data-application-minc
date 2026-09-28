-- Bronze SALIC — sac__dba_geral_2012.
-- Origem: salic_bronze.sac__dba_geral_2012, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 29 colunas: 1 tipadas, 27 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto('"nome do projeto"') }} as nome_do_projeto,
    {{ bronze_texto('"síntese do projeto"') }} as sintese_do_projeto,
    {{ bronze_texto('"valor solicitado"') }} as valor_solicitado,
    {{ bronze_texto('"valor aprovado"') }} as valor_aprovado,
    {{ bronze_texto('"valor captado"') }} as valor_captado,
    {{ bronze_texto('"cidade de abrangência"') }} as cidade_de_abrangencia,
    {{ bronze_texto('"uf de abrangência"') }} as uf_de_abrangencia,
    {{ bronze_texto('"área"') }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto('"situação"') }} as situacao,
    {{ bronze_texto('"data de início da execução"') }} as data_de_inicio_da_execucao,
    {{ bronze_texto('"data de término da execução"') }} as data_de_termino_da_execucao,
    {{ bronze_texto('"nome do proponente"') }} as nome_do_proponente,
    {{ bronze_texto('"tipo do proponente"') }} as tipo_do_proponente,
    {{ bronze_texto('"cnpj ou cpf do proponente"') }} as cnpj_ou_cpf_do_proponente,
    {{ bronze_texto('"cidade do proponente"') }} as cidade_do_proponente,
    {{ bronze_texto('"uf do proponente"') }} as uf_do_proponente,
    {{ bronze_texto('"e-mail do proponente"') }} as e_mail_do_proponente,
    {{ bronze_texto('"telefone comercial do proponente"') }}
    as telefone_comercial_do_proponente,
    {{ bronze_texto('"telefone celular do proponente"') }}
    as telefone_celular_do_proponente,
    {{ bronze_texto('"responsável pelo proponente"') }} as responsavel_pelo_proponente,
    {{ bronze_texto('"nome do investidor"') }} as nome_do_investidor,
    {{ bronze_texto('"tipo do investidor"') }} as tipo_do_investidor,
    {{ bronze_texto('"cnpj ou cpf do investidor"') }} as cnpj_ou_cpf_do_investidor,
    {{ bronze_texto('"cidade do investidor"') }} as cidade_do_investidor,
    {{ bronze_texto('"uf do investidor"') }} as uf_do_investidor,
    {{ bronze_texto('"valor investido"') }} as valor_investido,
    _fatia
from {{ source("bronze_sac", "sac__dba_geral_2012") }}
