-- Silver SALIC / core -- dimensao do projeto cultural na Lei Rouanet.
--
-- GRAO: 1 linha por PRONAC. A chave e `pronac`, texto de 7 posicoes.
--
-- UNIVERSO: todo PRONAC observado em qualquer tabela da bronze v2 que carregue
-- a chave (via `map_chave_projeto_rouanet`), e nao apenas os projetos que o
-- cadastro resolve. E deliberado: um projeto que so aparece em captacao existe
-- para efeito de indicador, e some-lo apenas quando o cadastro o descreve
-- produziria um denominador que muda sozinho conforme a carga avanca. Projeto
-- sem cadastro sai com os atributos descritivos nulos, nao fora da tabela.
--
-- IDENTIDADE FICA DE FORA. Nem CPF/CNPJ, nem nome de proponente, nem `logon`
-- de analista. A ligacao projeto -> proponente e a
-- `brg_projeto_proponente_rouanet`, que e zona restrita. Esta dimensao existe
-- justamente para ser a parte publicavel do projeto.
--
-- O QUE ESTA MODELADO COMO "ULTIMO". Aprovacao e enquadramento tem historico
-- no SALIC (readequacao, prorrogacao, reenquadramento). A dimensao guarda o
-- registro mais recente de cada um -- e a leitura que um painel de meta usa --
-- e o historico completo fica em `fct_aprovacao_rouanet`, que nao perde linha.
--
-- LIMITES CONHECIDOS, a fechar no gate G1 do plano (§13 da memoria de
-- OpenMetadata):
-- * `sac__vwalterarprojeto` e a visao mais completa do cadastro disponivel
-- na bronze v2 (`Projetos` da v2 ainda nao foi ingerida). O grao dela nao
-- foi medido contra o banco, por isso o `distinct on` e obrigatorio aqui,
-- e nao defensivo;
-- * o codigo de area vem como texto em `sac__area` e como inteiro nas views
-- de projeto; a ponte normaliza para digitos. Area alfanumerica, se
-- existir, resolve para nome nulo em vez de casar errado;
-- * a taxa de projetos sem cadastro resolvido ainda nao foi medida.
with

    chave as (
        select pronac, ano_pronac, id_pronac from {{ ref("map_chave_projeto_rouanet") }}
    ),

    cadastro_bruto as (
        select
            {{ pronac_normalizado("pronac") }} as pronac,
            nomeprojeto,
            area,
            segmento,
            situacao,
            dtsituacao,
            dtinicioexecucao,
            dtfimexecucao
        from {{ ref("sac__vwalterarprojeto") }}
    ),

    cadastro as (
        select distinct
            on (pronac)
            pronac,
            nomeprojeto,
            area as codigo_area,
            segmento as codigo_segmento,
            situacao as codigo_situacao,
            dtsituacao as data_situacao,
            dtinicioexecucao as data_inicio_execucao,
            dtfimexecucao as data_fim_execucao
        from cadastro_bruto
        where pronac is not null
        order by pronac asc, dtsituacao desc nulls last
    ),

    mecanismo_bruto as (
        select {{ pronac_normalizado("pronac") }} as pronac, mecanismo
        from {{ ref("sac__vwagentesseusprojetos") }}
    ),

    mecanismo as (
        select distinct on (pronac) pronac, mecanismo as codigo_mecanismo
        from mecanismo_bruto
        where pronac is not null and mecanismo is not null
        order by pronac asc, mecanismo asc
    ),

    enquadramento_bruto as (
        select
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            enquadramento,
            dtenquadramento
        from {{ ref("sac__enquadramento") }}
    ),

    enquadramento as (
        select distinct
            on (pronac)
            pronac,
            enquadramento as codigo_enquadramento,
            dtenquadramento::date as data_enquadramento
        from enquadramento_bruto
        where pronac is not null
        order by pronac asc, dtenquadramento desc nulls last
    ),

    aprovacao_bruta as (
        select
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            dtaprovacao,
            dtpublicacaoaprovacao,
            dtiniciocaptacao,
            dtfimcaptacao,
            aprovadoreal,
            autorizadoreal
        from {{ ref("sac__aprovacao") }}
    ),

    aprovacao_ultima as (
        select distinct
            on (pronac)
            pronac,
            dtaprovacao::date as data_ultima_aprovacao,
            dtpublicacaoaprovacao::date as data_publicacao_ultima_aprovacao,
            dtiniciocaptacao::date as data_inicio_captacao,
            dtfimcaptacao::date as data_fim_captacao,
            aprovadoreal as valor_aprovado_reais,
            autorizadoreal as valor_autorizado_reais
        from aprovacao_bruta
        where pronac is not null
        order by pronac asc, dtaprovacao desc nulls last
    ),

    aprovacao_historico as (
        select
            pronac,
            count(*) as qt_aprovacoes,
            min(dtaprovacao)::date as data_primeira_aprovacao
        from aprovacao_bruta
        where pronac is not null
        group by pronac
    ),

    area as (
        select
            descricao as nome_area,
            nullif(regexp_replace(codigo, '[^0-9]', '', 'g'), '')::integer as codigo_area
        from {{ ref("sac__area") }}
    ),

    segmento as (
        -- a chave do segmento e (area, codigo): o codigo se repete entre areas, e
        -- casar so por ele traria o nome do segmento de outra area.
        select distinct
            on (area, codigo)
            area as codigo_area,
            codigo as codigo_segmento,
            segmento as nome_segmento
        from {{ ref("sac__vsegmento") }}
        where area is not null and codigo is not null
        order by area asc, codigo asc, segmento asc
    )

select
    c.pronac,
    c.ano_pronac,
    c.id_pronac,
    cad.nomeprojeto as nome_projeto,
    cad.codigo_area,
    a.nome_area,
    cad.codigo_segmento,
    s.nome_segmento,
    m.codigo_mecanismo,
    e.codigo_enquadramento,
    e.data_enquadramento,
    cad.codigo_situacao,
    cad.data_situacao,
    cad.data_inicio_execucao,
    cad.data_fim_execucao,
    ap.data_primeira_aprovacao,
    ult.data_ultima_aprovacao,
    ult.data_publicacao_ultima_aprovacao,
    ult.data_inicio_captacao,
    ult.data_fim_captacao,
    ult.valor_aprovado_reais,
    ult.valor_autorizado_reais,
    coalesce(ap.qt_aprovacoes, 0) as qt_aprovacoes,
    coalesce(ap.projeto_aprovado, false) as projeto_aprovado,
    cad.pronac is not null as cadastro_resolvido
from chave as c
left join cadastro as cad on c.pronac = cad.pronac
left join mecanismo as m on c.pronac = m.pronac
left join enquadramento as e on c.pronac = e.pronac
left join aprovacao_ultima as ult on c.pronac = ult.pronac
left join aprovacao_historico as ap on c.pronac = ap.pronac
left join area as a on cad.codigo_area = a.codigo_area
left join
    segmento as s
    on cad.codigo_area = s.codigo_area
    and cad.codigo_segmento = s.codigo_segmento
