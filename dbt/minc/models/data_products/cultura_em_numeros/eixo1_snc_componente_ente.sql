-- Gold-base do Eixo 1, frente SNC: os componentes do sistema de cultura de cada
-- ente, uma linha por ente e componente (conselho, fundo, plano, lei do sistema
-- e CPF completo), na versão mais recente da lista de entes do SNC no portal de
-- dados abertos. É a base da Tabela 1.2 e das Figuras 12, 13 e 14 do
-- snc_figuras_tabelas.R, e do eda_10 do SNC.R (repositório SGE
-- cultura-em-numeros-eixo-1). As golds eixo1_snc_componente* leem daqui.
--
-- Universo: os 27 entes estaduais, com o DF (53), que no portal têm código IBGE
-- de 2 dígitos (a aba "SNC - ESTADOS" do R), e os municípios, com 7. A linha
-- 'Não cadastrado' do portal não é ente e fica de fora.
--
-- A regra, como no R: aderente é situação 'Publicado no DOU'; o ente tem o
-- componente quando a situação do documento é 'Concluída'; CPF completo é ter
-- conselho, plano e fundo. O ano da lei é o da data do documento (do plano, no
-- caso do plano), e ano antes de 1900 é erro de digitação e fica NULL, como no
-- extrair_ano do R.
--
-- Aderente e versão são a mesma regra de eixo1_snc_painel_municipio, que é só
-- dos municípios e não traz as datas das leis.
with

    versao_atual as (select max(versao) as versao from {{ ref("silver_snc_entes") }}),

    entes as (
        select
            e.versao,
            e.cod_ibge,
            e.ente_federado as ente,
            e.uf,
            e.regiao,
            e.situacao as situacao_adesao,
            e.situacao_da_lei_do_conselho_de_politica_cultural as situacao_conselho,
            e.data_da_lei_do_conselho_de_politica_cultural as data_conselho,
            e.situacao_da_lei_do_fundo_de_cultura as situacao_fundo,
            e.data_da_lei_do_fundo_de_cultura as data_fundo,
            e.situacao_do_plano_de_cultura as situacao_plano,
            e.data_do_plano_de_cultura as data_plano,
            e.situacao_da_lei_do_sistema_de_cultura as situacao_lei_sistema,
            e.data_da_lei_do_sistema_de_cultura as data_lei_sistema,
            case
                when e.cod_ibge ~ '^[0-9]{2}$' then 'estadual' else 'municipal'
            end as esfera,
            e.situacao = 'Publicado no DOU' as aderente
        from {{ ref("silver_snc_entes") }} as e
        inner join versao_atual as v on e.versao = v.versao
        where e.cod_ibge ~ '^[0-9]{2}$' or e.cod_ibge ~ '^[0-9]{7}$'
    ),

    componentes as (
        select
            versao,
            cod_ibge,
            ente,
            uf,
            regiao,
            esfera,
            situacao_adesao,
            aderente,
            'Conselho' as componente,
            situacao_conselho as situacao_componente,
            situacao_conselho = 'Concluída' as tem_componente,
            data_conselho as data_lei
        from entes
        union all
        select
            versao,
            cod_ibge,
            ente,
            uf,
            regiao,
            esfera,
            situacao_adesao,
            aderente,
            'Fundo' as componente,
            situacao_fundo as situacao_componente,
            situacao_fundo = 'Concluída' as tem_componente,
            data_fundo as data_lei
        from entes
        union all
        select
            versao,
            cod_ibge,
            ente,
            uf,
            regiao,
            esfera,
            situacao_adesao,
            aderente,
            'Plano' as componente,
            situacao_plano as situacao_componente,
            situacao_plano = 'Concluída' as tem_componente,
            data_plano as data_lei
        from entes
        union all
        select
            versao,
            cod_ibge,
            ente,
            uf,
            regiao,
            esfera,
            situacao_adesao,
            aderente,
            'Lei do sistema' as componente,
            situacao_lei_sistema as situacao_componente,
            situacao_lei_sistema = 'Concluída' as tem_componente,
            data_lei_sistema as data_lei
        from entes
        union all
        -- CPF completo não tem documento próprio: sem situação e sem data. Com
        -- a lógica de três valores do SQL, fica NULL só quando falta alguma
        -- situação e as conhecidas são 'Concluída' (ADR 0010, regra 1).
        select
            versao,
            cod_ibge,
            ente,
            uf,
            regiao,
            esfera,
            situacao_adesao,
            aderente,
            'CPF completo' as componente,
            null::text as situacao_componente,
            situacao_conselho = 'Concluída'
            and situacao_fundo = 'Concluída'
            and situacao_plano = 'Concluída' as tem_componente,
            null::date as data_lei
        from entes
    )

select
    versao,
    cod_ibge,
    ente,
    uf,
    regiao,
    esfera,
    situacao_adesao,
    aderente,
    componente,
    situacao_componente,
    tem_componente,
    data_lei,
    case
        when extract(year from data_lei) >= 1900 then extract(year from data_lei)::int
    end as ano_lei
from componentes
