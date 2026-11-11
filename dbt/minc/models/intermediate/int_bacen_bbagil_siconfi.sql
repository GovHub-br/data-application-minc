-- Intermediate — o gasto dos entes numa forma só, com o ente e o IPCA do ano:
-- a MSC orçamentária do SICONFI e o extrato bancário do BB Ágil, empilhados.
--
-- Cruza três fontes e não decide nada (ADR 0010, regra 2; ADR 0015, decisão 3):
-- • siconfi: cada linha de silver_msc_orcamentaria, sem filtro (base_origem
-- 'siconfi_msc'), e o nome, a UF, a esfera e a região do ente (silver_entes,
-- pelo cod_ibge da linha; a MSC não traz a UF);
-- • bbagil: cada lançamento de silver_extrato_bbagil, sem filtro, débito e
-- crédito, de todo programa (base_origem 'bbagil_extrato'). É de onde a Gold
-- tira a LAB 1 municipal, que a MSC não separa na fonte. O ano do lançamento
-- é o da data do lançamento (bookingdate);
-- • bacen: o IPCA (SGS 433, silver_serie_sgs) do exercício da linha, como
-- número-índice médio do ano.
--
-- UMA FORMA SÓ. As colunas da MSC ficam nulas na linha do extrato, e as do
-- extrato na linha da MSC; base_origem diz de qual das duas a linha veio. Qual
-- matriz, conta e função contam, qual programa é a LAB 1 e que lançamento é
-- gasto é regra de negócio e fica na Gold (eixo1_orcamento_empenho_cultura).
-- Do extrato não vem nenhuma coluna do beneficiário (nome, documento,
-- agência, conta): a Gold do orçamento não precisa delas.
--
-- O NÚMERO-ÍNDICE. A série 433 é a variação mensal em %. O índice encadeia as
-- variações desde o primeiro mês da série (produto de 1 + v/100) e a média do
-- ano é a média dos meses daquele ano que a série tem. O primeiro mês é uma
-- escala arbitrária: some na razão entre dois anos, que é o que a Gold usa para
-- deflacionar. A escolha do ano-base é da Gold. ipca_meses_ano diz quantos
-- meses entraram na média (12 no ano fechado).
--
-- VIEW, NÃO TABLE: a linha é a da MSC inteira, de todos os entes e recortes que
-- a bronze tiver, mais o extrato inteiro. Materializar duplicaria as silvers; a
-- Gold que filtra é que vira tabela.
--
-- O ente e o IPCA entram como CTE `materialized`, calculadas uma vez por
-- consulta. Os filtros da Gold descem até a MSC, o Postgres estima uma linha e
-- junta por nested loop; sem isso, refaria a silver_entes (e a série do IPCA)
-- para cada linha da MSC. Mesma razão da macro siconfi_tentativa_valida. O
-- filtro da Gold em base_origem também desce: em cada ramo do union all ele
-- vira uma constante, e o Postgres nem lê o ramo que não serve.
-- (O noqa: LT01 nelas é porque o sqlfmt cola o parêntese no `materialized`.)
{{ config(materialized="view") }}

with
    ipca_mensal as (
        select
            data,
            exp(
                sum(ln(1 + valor / 100)) over (
                    order by data rows between unbounded preceding and current row
                )
            ) as indice
        from {{ ref("silver_serie_sgs") }}
        where codigo_serie = '433'
    ),

    ipca_ano as materialized(  -- noqa: LT01
        select
            extract(year from data)::integer as exercicio,
            avg(indice) as ipca_indice_medio_ano,
            count(*) as ipca_meses_ano
        from ipca_mensal
        group by 1
    ),

    ente as materialized(  -- noqa: LT01
        select cod_ibge, ente, sigla_uf, esfera, regiao from {{ ref("silver_entes") }}
    ),

    msc as (
        select
            'siconfi_msc' as base_origem,
            cod_ibge,
            exercicio,
            mes_referencia,
            data_referencia,
            tipo_matriz,
            classe_conta,
            tipo_valor,
            conta_contabil,
            natureza_conta,
            poder_orgao,
            funcao,
            subfuncao,
            educacao_saude,
            natureza_despesa,
            natureza_receita,
            ano_fonte_recursos,
            fonte_recursos,
            complemento_fonte,
            ano_inscricao,
            entrada_msc,
            null::bigint as codigo_programa,
            null::integer as id_plano_acao,
            null::integer as id_plano_acao_dado_bancario,
            null::bigint as id_transacao,
            null::date as data_lancamento,
            null::text as creditdebitindicator,
            null::integer as descriptioncode,
            null::text as descriptionname,
            valor,
            _pagina_id,
            _item
        from {{ ref("silver_msc_orcamentaria") }}
    ),

    extrato_datado as (
        select *, {{ data_texto("bookingdate") }} as data_lancamento
        from {{ ref("silver_extrato_bbagil") }}
    ),

    extrato as (
        select
            'bbagil_extrato' as base_origem,
            cod_ibge::bigint as cod_ibge,
            extract(year from data_lancamento)::integer as exercicio,
            null::integer as mes_referencia,
            null::date as data_referencia,
            null::text as tipo_matriz,
            null::integer as classe_conta,
            null::text as tipo_valor,
            null::text as conta_contabil,
            null::text as natureza_conta,
            null::text as poder_orgao,
            null::text as funcao,
            null::text as subfuncao,
            null::text as educacao_saude,
            null::text as natureza_despesa,
            null::text as natureza_receita,
            null::integer as ano_fonte_recursos,
            null::text as fonte_recursos,
            null::text as complemento_fonte,
            null::integer as ano_inscricao,
            null::bigint as entrada_msc,
            codigo_programa,
            id_plano_acao,
            id_plano_acao_dado_bancario,
            id as id_transacao,
            data_lancamento,
            creditdebitindicator,
            descriptioncode,
            descriptionname,
            value as valor,
            null::text as _pagina_id,
            null::integer as _item
        from extrato_datado
    ),

    linhas as (
        select *
        from msc
        union all
        select *
        from extrato
    )

select
    linhas.base_origem,
    linhas.cod_ibge,
    ente.ente,
    ente.sigla_uf as uf,
    ente.esfera,
    ente.regiao,
    linhas.exercicio,
    linhas.mes_referencia,
    linhas.data_referencia,
    linhas.tipo_matriz,
    linhas.classe_conta,
    linhas.tipo_valor,
    linhas.conta_contabil,
    linhas.natureza_conta,
    linhas.poder_orgao,
    linhas.funcao,
    linhas.subfuncao,
    linhas.educacao_saude,
    linhas.natureza_despesa,
    linhas.natureza_receita,
    linhas.ano_fonte_recursos,
    linhas.fonte_recursos,
    linhas.complemento_fonte,
    linhas.ano_inscricao,
    linhas.entrada_msc,
    linhas.codigo_programa,
    linhas.id_plano_acao,
    linhas.id_plano_acao_dado_bancario,
    linhas.id_transacao,
    linhas.data_lancamento,
    linhas.creditdebitindicator,
    linhas.descriptioncode,
    linhas.descriptionname,
    linhas.valor,
    ipca.ipca_indice_medio_ano,
    ipca.ipca_meses_ano,
    linhas._pagina_id,
    linhas._item
from linhas
left join ente on linhas.cod_ibge = ente.cod_ibge
left join ipca_ano as ipca on linhas.exercicio = ipca.exercicio
