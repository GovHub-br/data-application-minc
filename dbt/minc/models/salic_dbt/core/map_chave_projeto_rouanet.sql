-- Silver SALIC / core -- mapa das tres chaves do projeto na Rouanet.
--
-- GRAO: 1 linha por PRONAC.
--
-- POR QUE ESTE MODELO EXISTE. O SALIC identifica o mesmo projeto de tres
-- jeitos, e nenhuma tabela da bronze v2 carrega os tres juntos:
--
-- * `pronac`    -- chave de negocio, ano com 2 posicoes + sequencial sem
-- padding artificial (5 a 7 posicoes no dado observado);
-- * `idpronac`  -- surrogate inteiro, usado pelas tabelas de analise;
-- * `idprojeto` -- surrogate mais antigo, e a chave de `sac__abrangencia`,
-- que e a unica fonte de local de realizacao do projeto.
--
-- Sem este mapa a Meta 4 (execucao fora das capitais, execucao por regiao) nao
-- tem como ligar abrangencia a PRONAC. Ele resolve isso observando os pares
-- que aparecem nas tabelas que carregam duas ou tres das chaves na mesma
-- linha, e -- o ponto importante -- marcando quando a origem discorda de si
-- mesma em vez de escolher um valor por conta propria.
--
-- FAIL-CLOSED: quando o mesmo PRONAC aparece com mais de um `idpronac` (ou
-- mais de um `idprojeto`), a coluna do id sai NULL e a flag `*_ambiguo` sai
-- true. Um join que nao acontece e visivel; um join com o id errado nao e.
--
-- LIMITE CONHECIDO: a cobertura deste mapa e a taxa de ambiguidade dependem do
-- que esta carregado no banco e ainda nao foram medidas (gate G1 do plano).
-- As colunas de contagem existem para que essa medicao seja uma consulta.
with

    observado as (

        select distinct
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            idpronac,
            cast(null as integer) as idprojeto,
            'sac__aprovacao' as tabela_origem
        from {{ ref("sac__aprovacao") }}

        union all

        select distinct
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            idpronac,
            cast(null as integer) as idprojeto,
            'sac__enquadramento' as tabela_origem
        from {{ ref("sac__enquadramento") }}

        union all

        -- a unica tabela transacional com as tres chaves na mesma linha
        select distinct
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            idpronac,
            nullif(idprojeto, 0) as idprojeto,
            'sac__documentosprojeto' as tabela_origem
        from {{ ref("sac__documentosprojeto") }}

        union all

        select distinct
            {{ pronac_normalizado("pronac") }} as pronac,
            idpronac,
            nullif(idprojeto, 0) as idprojeto,
            'sac__vwprojetositinerantes' as tabela_origem
        from {{ ref("sac__vwprojetositinerantes") }}

        union all

        select distinct
            {{ pronac_normalizado("pronac") }} as pronac,
            idpronac,
            cast(null as integer) as idprojeto,
            'sac__vwalterarprojeto' as tabela_origem
        from {{ ref("sac__vwalterarprojeto") }}

        union all

        select distinct
            {{ pronac_normalizado("pronac") }} as pronac,
            idpronac,
            cast(null as integer) as idprojeto,
            'sac__vwagentesseusprojetos' as tabela_origem
        from {{ ref("sac__vwagentesseusprojetos") }}

        union all

        -- `captacao.idprojeto` e 0 na maior parte das linhas. Sem o `nullif` esse
        -- zero viraria um "projeto 0" compartilhado por meio SALIC.
        select distinct
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            cast(null as integer) as idpronac,
            nullif(idprojeto, 0) as idprojeto,
            'sac__captacao' as tabela_origem
        from {{ ref("sac__captacao") }}

    ),

    consolidado as (
        select
            pronac,
            min(idpronac) as id_pronac_observado,
            min(idprojeto) as id_projeto_observado,
            count(distinct idpronac) as qt_id_pronac_distintos,
            count(distinct idprojeto) as qt_id_projeto_distintos,
            count(distinct tabela_origem) as qt_tabelas_origem
        from observado
        where pronac is not null
        group by pronac
    ),

    id_pronac_inverso as (
        select idpronac, count(distinct pronac) as qt_pronac_por_id_pronac
        from observado
        where pronac is not null and idpronac is not null
        group by idpronac
    ),

    id_projeto_inverso as (
        select idprojeto, count(distinct pronac) as qt_pronac_por_id_projeto
        from observado
        where pronac is not null and idprojeto is not null
        group by idprojeto
    )

select
    c.pronac,
    {{ ano_do_pronac("c.pronac") }} as ano_pronac,
    case
        when
            c.qt_id_pronac_distintos <= 1 and coalesce(ip.qt_pronac_por_id_pronac, 0) <= 1
        then c.id_pronac_observado
    end as id_pronac,
    case
        when
            c.qt_id_projeto_distintos <= 1
            and coalesce(ipro.qt_pronac_por_id_projeto, 0) <= 1
        then c.id_projeto_observado
    end as id_projeto,
    (
        c.qt_id_pronac_distintos > 1 or coalesce(ip.qt_pronac_por_id_pronac, 0) > 1
    ) as id_pronac_ambiguo,
    (
        c.qt_id_projeto_distintos > 1 or coalesce(ipro.qt_pronac_por_id_projeto, 0) > 1
    ) as id_projeto_ambiguo,
    coalesce(ip.qt_pronac_por_id_pronac, 0) as qt_pronac_por_id_pronac,
    coalesce(ipro.qt_pronac_por_id_projeto, 0) as qt_pronac_por_id_projeto,
    c.qt_tabelas_origem
from consolidado as c
left join id_pronac_inverso as ip on c.id_pronac_observado = ip.idpronac
left join id_projeto_inverso as ipro on c.id_projeto_observado = ipro.idprojeto
