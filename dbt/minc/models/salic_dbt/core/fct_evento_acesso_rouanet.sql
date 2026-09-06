-- Silver SALIC / core -- espinha de eventos datados de acesso a Rouanet.
--
-- GRAO: 1 linha por evento qualificavel. A chave e `id_evento`.
--
-- PARA QUE SERVE. A Meta 5 pergunta quantos proponentes acessaram a Rouanet
-- pela primeira vez em um ano. Responder isso exige uma serie de eventos
-- datados e comparaveis, e nao tres consultas separadas -- e exige que
-- "primeira aprovacao" e "primeira captacao" continuem distinguiveis, porque
-- sao conceitos diferentes e o plano proibe colapsa-los. Por isso o tipo do
-- evento e coluna, e nao um filtro embutido.
--
-- ESTE MODELO E DE PROJETO, NAO DE PESSOA. Ele nao carrega proponente,
-- documento nem id de agente. A ligacao com identidade acontece uma camada
-- acima, na zona restrita, cruzando com `brg_projeto_proponente_rouanet`.
-- Manter a espinha no grao do projeto e o que permite publica-la.
--
-- FALTA O EVENTO DE REGISTRO, e a ausencia e conhecida. O plano previa tres
-- tipos -- registro, aprovacao e captacao -- mas a data de registro da
-- proposta esta em `PreProjeto`/`Projetos` da v2, que nao foram ingeridas
-- (§13 da memoria de OpenMetadata lista as duas entre os objetos ausentes).
-- Derivar registro de `dtsituacao` seria errado: aquilo e a ultima mudanca de
-- situacao, nao a entrada. Enquanto a fonte nao chega, `tipo_evento` tem dois
-- valores, e qualquer KPI de "primeiro acesso" construido sobre isto mede
-- primeiro acesso A RECURSO, nao primeiro contato com o sistema.
--
-- DATA DO EVENTO DE APROVACAO. Publicacao vem antes de data de aprovacao no
-- `coalesce` porque e a data com efeito legal -- e a partir da publicacao que
-- o projeto pode captar. Quando a publicacao nao esta preenchida, a data de
-- aprovacao entra como substituta, e `data_evento_substituta` marca isso, para
-- que o gold possa medir o quanto da serie depende do substituto.
with

    captacao as (
        select
            'CAPTACAO' as tipo_evento,
            pronac,
            id_captacao as id_origem,
            data_recibo as data_evento,
            false as data_evento_substituta,
            valor_captado_reais as valor_reais
        from {{ ref("fct_captacao_rouanet") }}
    ),

    aprovacao as (
        select
            'APROVACAO' as tipo_evento,
            pronac,
            id_aprovacao as id_origem,
            coalesce(data_publicacao, data_aprovacao) as data_evento,
            data_publicacao is null as data_evento_substituta,
            valor_autorizado_reais as valor_reais
        from {{ ref("fct_aprovacao_rouanet") }}
    ),

    evento as (
        select *
        from captacao
        union all
        select *
        from aprovacao
    ),

    qualificado as (
        select
            *,
            tipo_evento || '-' || id_origem::text as id_evento,
            extract(year from data_evento)::integer as ano_evento
        from evento
        where data_evento is not null
    )

select
    id_evento,
    tipo_evento,
    pronac,
    id_origem,
    data_evento,
    ano_evento,
    data_evento_substituta,
    valor_reais
from qualificado
