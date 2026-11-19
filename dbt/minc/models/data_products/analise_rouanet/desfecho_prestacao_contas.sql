-- Gold / analise_rouanet -- desfecho da prestacao de contas de cada projeto.
--
-- GRAO: 1 linha por PRONAC. UNIVERSO: todo PRONAC de `silver_sac_projeto_rouanet`
-- (276.866 em 18/11/2026), inclusive os que nunca captaram; quem nao tem codigo de
-- desfecho sai 'sem desfecho', nao fora da tabela.
--
-- O SALIC NAO TEM UM CAMPO "RESULTADO DA PRESTACAO DE CONTAS". O desfecho aparece
-- como codigo de situacao, e a classe de cada codigo e a tabela `classe` abaixo,
-- o unico lugar onde a regra mora: o `historico_proponente` le as datas daqui.
-- A regra veio do notebook 04 do analytics_minc, lida da descricao de cada codigo
-- em `silver_sac_situacao`, e AINDA NAO FOI VALIDADA PELA AREA. E regra de produto
-- (ADR 0015, decisao 3), por isso e Gold e nao coluna do dicionario na Silver.
-- * a serie G (convenio do FNC) nao e incentivo fiscal e nao tem classe;
-- * inadimplencia e fase do processo, nao desfecho: o projeto sai dela quando
-- responde. Vira so a flag `passou_por_inadimplencia`;
-- * ficaram sem classe, de proposito, codigos que a regra do notebook nao cobre:
-- E21 (inadimplencia suspensa), E43 (relatorio aprovado UNB), R24 (apresentou
-- PC apos reprovacao), D56 (encerrado sem PC da edicao anterior). A area decide.
--
-- DUAS FONTES, A PIOR CLASSE VALE. A situacao atual (`codigo_situacao` do
-- projeto) e o historico (`silver_sac_historicosituacao`). O historico e
-- incompleto: registra aprovacao para menos de 1/4 dos projetos hoje aprovados,
-- e por isso as duas entram. Ordem: reprovada ou TCE > aprovada com prejuizo >
-- aprovada. Projeto reprovado e depois aprovado em recurso fica reprovado, e
-- `passou_por_reprovacao_e_aprovacao` marca o caso.
--
-- DATAS. Para cada classe, a primeira vez que ela aparece: no historico pela
-- `dtsituacao`, na situacao atual pela `data_situacao` do projeto. Data antes de
-- 1991 vira nula: e o 1900-01-01, sentinela de "sem data" do SQL Server (2 linhas
-- no historico e 7 na situacao atual, todas com codigo de classe). Classe sem data
-- valida em nenhuma fonte fica com a data nula, e o `historico_proponente`
-- nao a conta, porque nao sabe quando aconteceu.
--
-- O historico (5,25 mi de linhas, view sobre a Bronze) e filtrado pelos codigos
-- antes de qualquer agregacao: ficam 96.930 linhas. `providenciatomada` e
-- `logon` nao sao lidos.
with

    classe as (
        select codigo, classe, gravidade
        from
            (
                values
                    ('E19', 'aprovada', 1),
                    ('L01', 'aprovada', 1),
                    ('L02', 'aprovada', 1),
                    ('L03', 'aprovada', 1),
                    ('L04', 'aprovada com prejuízo', 2),
                    ('L08', 'aprovada com prejuízo', 2),
                    ('L05', 'reprovada ou TCE', 3),
                    ('L06', 'reprovada ou TCE', 3),
                    ('L07', 'reprovada ou TCE', 3),
                    ('L09', 'reprovada ou TCE', 3),
                    ('L10', 'reprovada ou TCE', 3),
                    ('L11', 'reprovada ou TCE', 3),
                    ('E22', 'reprovada ou TCE', 3),
                    ('E67', 'reprovada ou TCE', 3),
                    ('E73', 'reprovada ou TCE', 3),
                    ('E78', 'reprovada ou TCE', 3),
                    ('D42', 'reprovada ou TCE', 3),
                    -- gravidade 0: fase, nao desfecho
                    ('E20', 'inadimplência', 0),
                    ('E23', 'inadimplência', 0),
                    ('E38', 'inadimplência', 0),
                    ('E66', 'inadimplência', 0),
                    ('E69', 'inadimplência', 0),
                    ('E71', 'inadimplência', 0),
                    ('E80', 'inadimplência', 0),
                    ('E81', 'inadimplência', 0),
                    ('E82', 'inadimplência', 0),
                    ('D38', 'inadimplência', 0)
            ) as c(codigo, classe, gravidade)  -- noqa: LT01
    ),

    projeto as (
        select
            pronac,
            codigo_situacao,
            case
                when data_situacao >= '1991-01-01' then data_situacao::date
            end as data_situacao
        from {{ ref("silver_sac_projeto_rouanet") }}
    ),

    historico as (
        select
            {{ pronac_de_ano_sequencial("h.anoprojeto", "h.sequencial") }} as pronac,
            c.gravidade,
            case
                when h.dtsituacao >= '1991-01-01' then h.dtsituacao::date
            end as data_evidencia
        from {{ ref("silver_sac_historicosituacao") }} as h
        -- o join e o filtro: so as linhas dos codigos da tabela chegam ao group by
        inner join classe as c on h.situacao = c.codigo
    ),

    evidencia as (
        select pronac, gravidade, data_evidencia, true as no_historico, false as na_atual
        from historico
        where pronac is not null
        union all
        select
            p.pronac,
            c.gravidade,
            p.data_situacao as data_evidencia,
            false as no_historico,
            true as na_atual
        from projeto as p
        inner join classe as c on p.codigo_situacao = c.codigo
    ),

    por_gravidade as (
        select
            pronac,
            gravidade,
            min(data_evidencia) as primeira_data,
            bool_or(no_historico) as no_historico,
            bool_or(na_atual) as na_atual
        from evidencia
        group by pronac, gravidade
    ),

    por_projeto as (
        select
            pronac,
            max(gravidade) filter (where gravidade > 0) as pior_gravidade,
            min(primeira_data) filter (where gravidade = 1) as data_primeira_aprovada,
            min(primeira_data) filter (where gravidade = 2) as data_primeira_com_prejuizo,
            min(primeira_data) filter (where gravidade = 3) as data_primeira_reprovada,
            bool_or(gravidade = 0) as passou_por_inadimplencia,
            bool_or(gravidade in (1, 2))
            and bool_or(gravidade = 3) as passou_por_reprovacao_e_aprovacao
        from por_gravidade
        group by pronac
    )

-- noqa: disable=ST06
-- a ordem das colunas e a do pedido do analytics_minc, nao a do estilo
select
    p.pronac,
    case
        pp.pior_gravidade
        when 3
        then 'reprovada ou TCE'
        when 2
        then 'aprovada com prejuízo'
        when 1
        then 'aprovada'
        else 'sem desfecho'
    end as desfecho_pc,
    pior.primeira_data as data_desfecho,
    -- nulo so quando nao ha nenhuma das duas fontes: sem codigo atual e sem
    -- linha de classe no historico
    case
        when pp.pronac is not null
        then pp.passou_por_inadimplencia
        when p.codigo_situacao is not null
        then false
    end as passou_por_inadimplencia,
    case
        when pp.pronac is not null
        then pp.passou_por_reprovacao_e_aprovacao
        when p.codigo_situacao is not null
        then false
    end as passou_por_reprovacao_e_aprovacao,
    case
        when pior.no_historico and pior.na_atual
        then 'ambos'
        when pior.no_historico
        then 'histórico'
        when pior.na_atual
        then 'situação atual'
    end as fonte,
    pp.data_primeira_aprovada,
    pp.data_primeira_com_prejuizo,
    pp.data_primeira_reprovada
from projeto as p
left join por_projeto as pp on p.pronac = pp.pronac
left join
    por_gravidade as pior
    on pp.pronac = pior.pronac
    and pp.pior_gravidade = pior.gravidade
