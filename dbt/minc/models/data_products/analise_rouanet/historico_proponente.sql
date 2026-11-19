-- Gold / analise_rouanet -- historico do proponente de cada projeto, sem identidade.
--
-- GRAO: 1 linha por PRONAC. UNIVERSO: os PRONACs de `silver_sac_projeto_rouanet`
-- que tem data de referencia (foram aprovados) e agente na ponte de proponente.
-- Os outros nao tem como ter historico medido e ficam fora: quem cruza por
-- `left join` recebe nulo, que e desconhecido. Zero quer dizer proponente
-- conhecido sem nenhum projeto anterior.
--
-- DATA DE REFERENCIA: a primeira aprovacao DESTE PRONAC. Cada projeto anterior so
-- conta pelo que ja tinha acontecido ANTES dela (estritamente), medida a medida:
-- * existia: tem alguma prova datada antes da referencia. PRONAC de ano civil
-- anterior ao da referencia, ou aprovacao, recibo ou classe de prestacao de
-- contas datados antes dela. A ordem dos PRONACs nao serve: em 807 dos 181.292
-- PRONACs aprovados a primeira aprovacao e de ano anterior ao do proprio PRONAC,
-- entao o ano do PRONAC nao data o cadastro (assume-se so que ele nunca e
-- anterior ao cadastro), e a data de cadastro nao esta na Bronze (`Projetos` da
-- v2 nao foi ingerida);
-- * aprovado: primeira aprovacao antes da referencia;
-- * captou: soma dos recibos com data antes da referencia maior que zero, com
-- estorno descontado. O valor soma so os projetos que captaram, a mesma regra
-- da contagem, em reais nominais;
-- * prestacao de contas: a pior classe de `desfecho_prestacao_contas` cuja
-- primeira data e anterior a referencia. Projeto aprovado em 2010 e reprovado
-- em 2018 conta como aprovado para uma referencia de 2012. Classe sem data
-- valida nao conta;
-- * anos desde o primeiro projeto: ano da referencia menos o menor ano entre as
-- provas acima, que nunca e posterior a referencia.
-- Projeto anterior com desfecho posterior a referencia nao entra em nada.
--
-- QUEM E O PROPONENTE. A ligacao projeto -> agente e a ponte restrita
-- `silver_sac_projeto_proponente_rouanet`. Daqui so le `id_agente`, para achar os
-- outros projetos do mesmo agente; nenhum documento, nome ou id sai. Em 33 PRONACs
-- a ponte liga dois agentes: os projetos anteriores sao a uniao dos dois, sem
-- contar duas vezes o mesmo. A ponte nao garante que todo agente seja proponente,
-- e nao outro papel (limitacao dela, gate G1).
--
-- PF E PJ, SEM SUPRESSAO. Cada linha e um projeto, nao uma celula agregada de
-- pessoas, e o que ela diz (quantos projetos o proponente teve, quanto captou)
-- ja e publico no SALIC para quem sabe o proponente do PRONAC. Suprimir contagem
-- pequena apagaria a variavel justamente para os 40.928 agentes de um projeto so.
-- O limiar de supressao (k < 10 sugerido) segue aberto para o DPO no ADR 0006 e
-- vale para resultado demografico; o modelo fica `Disabled` e so e publicavel
-- depois da validacao de seguranca.
with

    projeto as (
        select pronac, ano_pronac, data_primeira_aprovacao
        from {{ ref("silver_sac_projeto_rouanet") }}
    ),

    vinculo as (
        select distinct pronac, id_agente
        from {{ ref("silver_sac_projeto_proponente_rouanet") }}
    ),

    com_proponente as (select distinct pronac from vinculo),

    par as (
        select distinct este.pronac, outro.pronac as pronac_outro
        from vinculo as este
        inner join
            vinculo as outro
            on este.id_agente = outro.id_agente
            and este.pronac <> outro.pronac
    ),

    par_datado as (
        select
            par.pronac,
            par.pronac_outro,
            este.data_primeira_aprovacao as data_referencia,
            outro.ano_pronac as ano_pronac_outro,
            outro.data_primeira_aprovacao as data_aprovacao_outro,
            d.data_primeira_aprovada,
            d.data_primeira_com_prejuizo,
            d.data_primeira_reprovada
        from par
        inner join projeto as este on par.pronac = este.pronac
        inner join projeto as outro on par.pronac_outro = outro.pronac
        left join
            {{ ref("desfecho_prestacao_contas") }} as d on par.pronac_outro = d.pronac
        where este.data_primeira_aprovacao is not null
    ),

    captacao_dia as (
        select pronac, data_recibo, sum(valor_captado_reais) as valor
        from {{ ref("silver_sac_captacao_rouanet") }}
        group by pronac, data_recibo
    ),

    captacao_antes as (
        select
            pd.pronac,
            pd.pronac_outro,
            sum(c.valor) as valor,
            min(c.data_recibo) as primeiro_recibo
        from par_datado as pd
        inner join
            captacao_dia as c
            on pd.pronac_outro = c.pronac
            and pd.data_referencia > c.data_recibo
        group by pd.pronac, pd.pronac_outro
    ),

    -- cada prova so vale se datada antes da referencia; nula quando nao ha
    par_provas as (
        select
            pd.pronac,
            pd.data_referencia,
            ca.valor as valor_captado_antes,
            ca.primeiro_recibo,
            case
                when pd.ano_pronac_outro < extract(year from pd.data_referencia)
                then pd.ano_pronac_outro
            end as ano_pronac_anterior,
            case
                when pd.data_aprovacao_outro < pd.data_referencia
                then pd.data_aprovacao_outro
            end as aprovacao_antes,
            case
                when pd.data_primeira_reprovada < pd.data_referencia
                then 3
                when pd.data_primeira_com_prejuizo < pd.data_referencia
                then 2
                when pd.data_primeira_aprovada < pd.data_referencia
                then 1
            end as gravidade_pc_antes,
            least(
                case
                    when pd.data_primeira_aprovada < pd.data_referencia
                    then pd.data_primeira_aprovada
                end,
                case
                    when pd.data_primeira_com_prejuizo < pd.data_referencia
                    then pd.data_primeira_com_prejuizo
                end,
                case
                    when pd.data_primeira_reprovada < pd.data_referencia
                    then pd.data_primeira_reprovada
                end
            ) as primeira_pc_antes
        from par_datado as pd
        left join
            captacao_antes as ca
            on pd.pronac = ca.pronac
            and pd.pronac_outro = ca.pronac_outro
    ),

    -- o projeto anterior existia se ha ao menos uma prova; o ano de inicio e o
    -- menor ano entre elas, e nenhuma e posterior a referencia
    par_medido as (
        select
            *,
            least(
                ano_pronac_anterior,
                extract(year from aprovacao_antes)::integer,
                extract(year from primeiro_recibo)::integer,
                extract(year from primeira_pc_antes)::integer
            ) as ano_inicio_outro
        from par_provas
    ),

    historico as (
        select
            pronac,
            count(ano_inicio_outro) as qt_projetos_anteriores,
            count(aprovacao_antes) as qt_aprovados_anteriores,
            count(*) filter (where valor_captado_antes > 0) as qt_captaram_anteriores,
            sum(valor_captado_antes) filter (
                where valor_captado_antes > 0
            ) as valor_captado_anterior_reais,
            count(*) filter (where gravidade_pc_antes = 1) as qt_pc_aprovadas_anteriores,
            count(*) filter (
                where gravidade_pc_antes in (2, 3)
            ) as qt_pc_com_problema_anteriores,
            min(ano_inicio_outro) as ano_primeiro_projeto
        from par_medido
        group by pronac
    )

select
    p.pronac,
    p.data_primeira_aprovacao as data_referencia,
    coalesce(h.qt_projetos_anteriores, 0) as qt_projetos_anteriores,
    coalesce(h.qt_aprovados_anteriores, 0) as qt_aprovados_anteriores,
    coalesce(h.qt_captaram_anteriores, 0) as qt_captaram_anteriores,
    coalesce(h.valor_captado_anterior_reais, 0) as valor_captado_anterior_reais,
    coalesce(h.qt_pc_aprovadas_anteriores, 0) as qt_pc_aprovadas_anteriores,
    coalesce(h.qt_pc_com_problema_anteriores, 0) as qt_pc_com_problema_anteriores,
    coalesce(
        extract(year from p.data_primeira_aprovacao)::integer - h.ano_primeiro_projeto, 0
    ) as anos_desde_primeiro_projeto
from projeto as p
inner join com_proponente as cp on p.pronac = cp.pronac
left join historico as h on p.pronac = h.pronac
where p.data_primeira_aprovacao is not null
