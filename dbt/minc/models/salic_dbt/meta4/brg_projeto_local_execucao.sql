-- Silver SALIC / Meta 4 -- locais declarados de realizacao do projeto.
--
-- GRAO: 1 linha por `id_abrangencia`, o registro territorial do SALIC.
-- Nenhum valor financeiro e replicado ou rateado aqui. A regra de projeto
-- multicidade segue aberta e so sera aplicada em fato/gold depois de homologada.
--
-- GEOGRAFIA: este modelo representa local de realizacao, nao residencia do
-- proponente nem do prestador. Essa separacao segue o art. 15, par. 2, da IN
-- MinC 10/2023 como definicao operacional de papel geografico, sem afirmar que
-- a norma da PNAB cria obrigacao ou meta para a Lei Rouanet.
--
-- CHAVE: `sac__abrangencia` usa `idprojeto`; o mapa conformado recupera PRONAC.
-- Quando um `idprojeto` aponta para mais de um PRONAC, nenhum e escolhido e a
-- localidade permanece com `pronac_resolvido = false`.
--
-- TERRITORIO VULNERAVEL: a classificacao nao e feita aqui. O art. 15 lista 13
-- categorias, mas varias exigem limiar ou fonte externa ainda nao homologados.
-- Codigo ausente/desconhecido permanece nulo, nunca vira `false`.
with

    chave_por_id_projeto as (
        select id_projeto, min(pronac) as pronac
        from {{ ref("map_chave_projeto_rouanet") }}
        where id_projeto is not null
        group by id_projeto
        having count(*) = 1
    )

select  -- noqa: ST06
    m.pronac,
    a.idabrangencia as id_abrangencia,
    a.idprojeto as id_projeto,
    m.pronac is not null as pronac_resolvido,
    a.idpais as id_pais_origem,
    a.iduf as id_uf_origem,
    a.idmunicipioibge as codigo_municipio_ibge_origem,
    a.stabrangencia as registro_ativo,
    a.siabrangencia as codigo_realizacao,
    case a.siabrangencia when '2' then true when '1' then false end as local_realizado,
    case
        a.siabrangencia
        when '0'
        then 'SEM_INFORMACAO'
        when '1'
        then 'NAO_REALIZADO'
        when '2'
        then 'REALIZADO'
        else 'CODIGO_DESCONHECIDO'
    end as situacao_realizacao,
    a.dtiniciorealizacao::date as data_inicio_realizacao,
    a.dtfimrealizacao::date as data_fim_realizacao
from {{ ref("sac__abrangencia") }} as a
left join chave_por_id_projeto as m on a.idprojeto = m.id_projeto
