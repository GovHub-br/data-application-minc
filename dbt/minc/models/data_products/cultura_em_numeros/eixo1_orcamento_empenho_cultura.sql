-- Gold-base — o gasto em cultura de estados e municípios, linha a linha, com a
-- origem do recurso e o valor em reais de 2024. É onde mora a regra de negócio
-- do orçamento do Eixo 1 (ADR 0015, decisões 3 e 4): as golds de consumo
-- eixo1_orcamento_* leem daqui e só agregam.
--
-- Lê int_bacen_bbagil_siconfi, que só empilha a MSC e o extrato do BB Ágil, com
-- o ente e o IPCA. Reproduz a regra de gasto_estadual.R e gasto_municipal.R
-- (repositório cultura-em-numeros-eixo-1, commit d28de40). Dois ramos:
--
-- 1. EMPENHO DA MSC (base_origem 'siconfi_msc'), as duas esferas:
-- • a MSC de dezembro, classe 6, saldo final, na matriz agregada (MSCC): o
-- saldo acumulado do ano. A MSCE, que a API também tem em 2025, fica fora;
-- • natureza do saldo credora (C) e conta contábil começando com 62213
-- (crédito empenhado: a liquidar, em liquidação, liquidado a pagar e pago);
-- • o valor em módulo, linha a linha, como o R (abs(valor)).
--
-- 2. LAB 1 MUNICIPAL (base_origem 'bbagil_extrato'). A MSC não separa a LAB 1
-- na fonte do município; o R a tira do cubo do BB Ágil (cubo_execucao_lab1_
-- municipios), e aqui ela vem do extrato das mesmas contas:
-- • o programa 30882120200002 (Aldir Blanc Municípios, Lei 14.017/2020);
-- • todo lançamento que não é crédito (indicador diferente de 'C', o nulo
-- incluído), como o R;
-- • menos os movimentos de aplicação financeira, que não são gasto: 271
-- ("BB-APLIC C.PRZ-APL.AUT"), 345 ("Aplicação em BB Fix") e 248 ("Estorno
-- Resgate Automático"). O cubo do R não os tem; com eles, o débito de 2020
-- quase empata com o crédito, pelo vai e vem entre conta e aplicação;
-- • o exercício é o ano da data do lançamento (bookingdate), como o R, e só
-- o ano fechado: o que tem os 12 meses do IPCA. O R lê o IPCA até dez/2025
-- e descarta a linha sem deflator;
-- • o valor como o extrato manda, sem módulo (o R soma o value);
-- • o DF recebe a LAB 1 dos municípios com o código da UF, 53. Aqui ele vira
-- o de Brasília (5300108), como no R, e entra na esfera municipal (na MSC o
-- DF fica fora do municipal).
--
-- DE ONDE VEIO O RECURSO (origem), na MSC, pela fonte sem pontos, nesta ordem:
-- • pnab: fonte começando com 1719, 2719, 1720, 2720, 719 ou 720;
-- • lpg: 1715, 2715, 1716, 2716, 715 ou 716;
-- • lab: só na esfera estadual e só em 2020 e 2021, fonte começando com 17
-- ou 27 (e não com 19 ou 29). Na municipal a MSC não a separa, e a LAB 1
-- vem do ramo 2, com origem 'lab';
-- • emenda: o complemento da fonte, só os dígitos e com zero à esquerda até
-- 4, está em 3101, 3110, 3111, 3120, 3121, 3130, 3140, 3201, 3202, 3210,
-- 3211, 3220 ou 3221 (emendas parlamentares);
-- • proprio: o resto (recurso do próprio ente).
--
-- QUEM CONTA COMO CULTURA: na MSC, a função 13 (Cultura), e, na esfera
-- estadual, a linha da LAB em qualquer função (o R estadual não exige a função
-- 13 da LAB). A LAB 1 municipal é cultura por definição do programa.
--
-- ESFERA: estadual é o ente de código IBGE de 2 dígitos, os 26 estados e o
-- Distrito Federal (no SICONFI, esfera 'E' e 'D'); municipal é o de 7 dígitos.
-- O DF não entra na municipal da MSC.
--
-- VALOR REAL: valor nominal vezes o número-índice médio do IPCA do ano-base
-- (var eixo1_orcamento_ano_base_ipca, padrão 2024) dividido pelo do exercício.
-- O índice vem de int_bacen_bbagil_siconfi; o do ano-base é lido das próprias
-- linhas daquele ano. Sem IPCA para o exercício, o valor real fica nulo.
--
-- GRÃO: uma linha por item da MSC que conta, (_pagina_id, _item), ou por
-- lançamento da LAB 1 que conta, (id_plano_acao, id_plano_acao_dado_bancario,
-- id_transacao).
{% set ano_base = var("eixo1_orcamento_ano_base_ipca", 2024) %}

with
    empenho as (
        select
            *,
            replace(fonte_recursos, '.', '') as fonte_sem_ponto,
            regexp_replace(complemento_fonte, '[^0-9]', '', 'g') as complemento_digitos,
            substring(funcao from '[0-9]+')::integer as funcao_numero,
            case
                when cod_ibge between 10 and 99
                then 'estadual'
                when cod_ibge between 1000000 and 9999999
                then 'municipal'
            end as esfera_eixo1
        from {{ ref("int_bacen_bbagil_siconfi") }}
        where
            base_origem = 'siconfi_msc'
            and tipo_matriz = 'MSCC'
            and mes_referencia = 12
            and classe_conta = 6
            and tipo_valor = 'ending_balance'
            and natureza_conta = 'C'
            and replace(conta_contabil, '.', '') like '62213%'
    ),

    classificado as (
        select
            *,
            case
                when fonte_sem_ponto ~ '^(1719|2719|1720|2720|719|720)'
                then 'pnab'
                when fonte_sem_ponto ~ '^(1715|2715|1716|2716|715|716)'
                then 'lpg'
                when
                    esfera_eixo1 = 'estadual'
                    and exercicio in (2020, 2021)
                    and fonte_sem_ponto !~ '^(19|29)'
                    and fonte_sem_ponto ~ '^(17|27)'
                then 'lab'
                -- str_pad do R completa com zero à esquerda até 4 e não corta.
                when
                    case
                        when length(complemento_digitos) >= 4
                        then complemento_digitos
                        else lpad(complemento_digitos, 4, '0')
                    end in (
                        '3101',
                        '3110',
                        '3111',
                        '3120',
                        '3121',
                        '3130',
                        '3140',
                        '3201',
                        '3202',
                        '3210',
                        '3211',
                        '3220',
                        '3221'
                    )
                then 'emenda'
                else 'proprio'
            end as origem
        from empenho
        where
            esfera_eixo1 = 'estadual'
            or (esfera_eixo1 = 'municipal' and uf is distinct from 'DF')
    ),

    msc_cultura as (
        select
            base_origem,
            esfera_eixo1,
            cod_ibge,
            ente,
            uf,
            regiao,
            exercicio,
            origem,
            funcao,
            subfuncao,
            poder_orgao,
            natureza_despesa,
            fonte_recursos,
            complemento_fonte,
            conta_contabil,
            abs(valor) as valor_nominal,
            ipca_indice_medio_ano,
            ipca_meses_ano,
            _pagina_id,
            _item,
            id_plano_acao,
            id_plano_acao_dado_bancario,
            id_transacao,
            data_lancamento,
            descriptioncode
        from classificado
        where funcao_numero = 13 or (esfera_eixo1 = 'estadual' and origem = 'lab')
    ),

    -- O extrato sai inteiro, materializado, antes dos filtros da LAB 1. Eles caem
    -- em colunas tipadas por CASE e regex, sem estatística: se descessem até o
    -- extrato, o Postgres estimaria poucas linhas e juntaria o ente por nested
    -- loop, um lançamento por vez (no recorte local, mais de 13 min contra 30 s).
    -- O filtro em base_origem desce e tira o ramo da MSC.
    extrato as materialized(  -- noqa: LT01
        select *
        from {{ ref("int_bacen_bbagil_siconfi") }}
        where base_origem = 'bbagil_extrato'
    ),

    lab1_municipal as (
        select
            base_origem,
            'municipal' as esfera_eixo1,
            case when cod_ibge = 53 then 5300108 else cod_ibge end as cod_ibge,
            ente,
            uf,
            regiao,
            exercicio,
            'lab' as origem,
            funcao,
            subfuncao,
            poder_orgao,
            natureza_despesa,
            fonte_recursos,
            complemento_fonte,
            conta_contabil,
            valor as valor_nominal,
            ipca_indice_medio_ano,
            ipca_meses_ano,
            _pagina_id,
            _item,
            id_plano_acao,
            id_plano_acao_dado_bancario,
            id_transacao,
            data_lancamento,
            descriptioncode
        from extrato
        where
            codigo_programa = 30882120200002
            and creditdebitindicator is distinct from 'C'
            and (descriptioncode is null or descriptioncode not in (248, 271, 345))
            and ipca_meses_ano = 12
    ),

    juntos as (
        select *
        from msc_cultura
        union all
        select *
        from lab1_municipal
    ),

    cultura as (
        select
            *,
            max(ipca_indice_medio_ano) filter (where exercicio = {{ ano_base }}) over (
            ) as ipca_indice_medio_base
        from juntos
    )

select
    base_origem,
    esfera_eixo1,
    cod_ibge,
    ente,
    uf,
    regiao,
    exercicio,
    origem,
    funcao,
    subfuncao,
    poder_orgao,
    natureza_despesa,
    fonte_recursos,
    complemento_fonte,
    conta_contabil,
    valor_nominal,
    ipca_indice_medio_base / ipca_indice_medio_ano as fator_ipca,
    valor_nominal * ipca_indice_medio_base / ipca_indice_medio_ano as valor_real,
    {{ ano_base }} as ano_base_ipca,
    ipca_meses_ano,
    _pagina_id,
    _item,
    id_plano_acao,
    id_plano_acao_dado_bancario,
    id_transacao,
    data_lancamento,
    descriptioncode
from cultura
