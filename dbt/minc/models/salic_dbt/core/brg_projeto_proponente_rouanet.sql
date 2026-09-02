-- Silver SALIC / core -- ponte entre projeto e agente proponente na Rouanet.
--
-- GRAO: 1 linha por PRONAC x agente. A chave e `id_projeto_proponente`.
--
-- ZONA RESTRITA. Este e o unico modelo do nucleo que carrega identidade:
-- CPF/CNPJ e nome do proponente. Ele existe separado da `dim_projeto_rouanet`
-- exatamente para que o projeto possa ser publicado sem arrastar a pessoa
-- junto. Nao entra em corpus de RAG, em nenhuma projecao, e nem por allowlist
-- de coluna -- a politica do plano e fail-closed no modelo inteiro.
--
-- POR QUE UMA PONTE, E NAO UMA COLUNA NA DIMENSAO. Um projeto pode ter mais de
-- um agente ligado a ele, e o vinculo tem semantica propria. Guardar o
-- proponente como atributo do projeto obrigaria a escolher um, em silencio, e
-- e essa escolha que muda a contagem de "proponentes distintos" da Meta 3 e da
-- Meta 5.
--
-- IDENTIDADE ENTRE BASES. A chave de cruzamento com as outras politicas
-- (LPG/PNAB/BB Agil) ainda NAO esta aqui. O plano decidiu HMAC-SHA256 com
-- segredo gerenciado, porque hash simples de CPF/CNPJ e enumeravel; enquanto o
-- segredo nao existir, esta ponte guarda apenas o documento normalizado, na
-- zona restrita, e nenhum modelo publicavel o consome.
--
-- LIMITES CONHECIDOS, a fechar no gate G1:
-- * `sac__vwagentesseusprojetos` nao esta no dicionario original do SALIC.
-- O grao (PRONAC x agente) e a leitura do nome da view e das colunas, nao
-- uma medicao; o `distinct on` garante o grao declarado independentemente
-- do que a origem entregue;
-- * `descricao_vinculo` chega como texto livre da origem. E o unico campo
-- que distingue o papel do agente no projeto, por isso fica -- mas o
-- dominio de valores nao foi levantado, e nenhum filtro depende dele
-- ainda;
-- * a view expoe o agente ligado ao projeto. Que todo agente listado seja
-- proponente, e nao outro papel, e hipotese a confirmar antes de qualquer
-- KPI contar "proponentes" a partir daqui.
with

    vinculo_bruto as (
        select
            {{ pronac_normalizado("pronac") }} as pronac,
            idagente as id_agente,
            {{ normaliza_documento("cgccpf") }} as documento_proponente,
            {{ tipo_pessoa_documento("cgccpf") }} as tipo_pessoa_proponente,
            nomeproponente as nome_proponente,
            idsolicitante as id_solicitante,
            descricao as descricao_vinculo
        from {{ ref("sac__vwagentesseusprojetos") }}
    ),

    vinculo as (
        select distinct
            on (pronac, id_agente)
            pronac,
            id_agente,
            documento_proponente,
            tipo_pessoa_proponente,
            nome_proponente,
            id_solicitante,
            descricao_vinculo
        from vinculo_bruto
        where pronac is not null and id_agente is not null
        order by pronac asc, id_agente asc, documento_proponente asc nulls last
    ),

    com_chave as (
        select
            *,
            pronac || '-' || id_agente::text as id_projeto_proponente,
            tipo_pessoa_proponente is not null as documento_valido
        from vinculo
    )

select
    id_projeto_proponente,
    pronac,
    id_agente,
    documento_proponente,
    documento_valido,
    tipo_pessoa_proponente,
    nome_proponente,
    id_solicitante,
    descricao_vinculo
from com_chave
