-- Gold-base do Eixo 1, frente MUNIC: o painel histórico da gestão municipal de
-- cultura, uma linha por município e edição da MUNIC (2006, 2014, 2018, 2021).
--
-- Reproduz o df_painel_historico do MUNIC.R (seções 3 e 4), que grava
-- munic_cultura_painel_historico_06_21.csv. A regra de negócio está toda aqui:
-- qual pergunta de cada edição responde cada variável, como cada resposta vira
-- 'Sim' ou 'Não' (macro eixo1_sim_nao) e as faixas de idade, raça, escolaridade e
-- execução orçamentária. As silvers do ibge só tipam.
--
-- Exceção à regra transversal 1 do ADR 0010, para bater com a publicação: '-',
-- 'Recusa' e 'Não informou' viram 'Não' na macro eixo1_sim_nao, como no R (as
-- contagens estão no schema.yml). Resposta ausente fica NULL; o R a transformava
-- em 'Não' ou 'Sem Informação'. Só acontece quando a aba de 2006 ou 2014 não tem
-- o município.
with

    munic_2021 as (
        select
            codmun::text as cod_municipio,
            mun as municipio,
            2021 as ano,
            pop as populacao,
            mcul01 as tipo_orgao_gestor,
            {{ eixo1_sim_nao("mcul10") }} as tem_plano,
            {{ eixo1_sim_nao("mcul19") }} as tem_conselho,
            {{ eixo1_sim_nao("mcul33") }} as tem_fundo,
            {{ eixo1_sim_nao("mcul15") }} as tem_lei_patrimonio,
            {{ eixo1_sim_nao("mcul26") }} as tem_cons_patrimonio,
            {{ eixo1_sim_nao("mcul3901") }} as equip_biblioteca,
            {{ eixo1_sim_nao("mcul3902") }} as equip_museu,
            {{ eixo1_sim_nao("mcul3903") }} as equip_teatro,
            {{ eixo1_sim_nao("mcul3909") }} as equip_cinema,
            mcul03 as gestor_sexo,
            mcul04 as gestor_idade,
            mcul05 as gestor_cor_raca,
            mcul06 as gestor_escolaridade,
            case
                when mcul101a is null
                then null
                when mcul101a ~ '[0-9]'
                then 'Sim'
                else 'Não'
            end as plano_tem_lei,
            {{ eixo1_sim_nao("mcul11") }} as plano_participacao_soc,
            {{ eixo1_sim_nao("mcul1111") }} as sec_educacao,
            {{ eixo1_sim_nao("mcul1112") }} as sec_esporte,
            {{ eixo1_sim_nao("mcul1113") }} as sec_turismo,
            case
                when mcul22 ~ '^-?[0-9]+(\.[0-9]+)?$' then mcul22::numeric
            end as cons_qtd_membros,
            {{ eixo1_sim_nao("mcul231") }} as cons_cap_continuada,
            {{ eixo1_sim_nao("mcul232") }} as cons_cap_eventual,
            {{ eixo1_sim_nao("mcul233") }} as cons_cap_nao_realiza,
            case
                when mcul41 is null
                then null
                when mcul41 ~* '^0$'
                then '0%'
                when mcul41 ~* 'at. 10'
                then 'Até 10%'
                when mcul41 ~* '11.*20'
                then '11% a 20%'
                when mcul41 ~* '21.*30'
                then '21% a 30%'
                when mcul41 ~* '31.*40'
                then '31% a 40%'
                when mcul41 ~* '41.*50'
                then '41% a 50%'
                when mcul41 ~* '51.*60'
                then '51% a 60%'
                when mcul41 ~* '61.*70'
                then '61% a 70%'
                when mcul41 ~* '71.*80'
                then '71% a 80%'
                when mcul41 ~* '81.*90'
                then '81% a 90%'
                when mcul41 ~* 'mais de 90'
                then 'Mais de 90%'
                else 'Sem Informação'
            end as orcamento_perc_executado
        from {{ ref("silver_munic_2021_cultura") }}
    ),

    -- Em 2018 o R não traz população, nome nem execução orçamentária. Os
    -- equipamentos de 2018, 2014 e 2006 o R guarda crus e o gráfico 11 conta só
    -- 'Sim' e 'Não'; aqui o resto ('-', 'Recusa', 'Não informou') já vira NULL.
    munic_2018 as (
        select
            codmun::text as cod_municipio,
            null::text as municipio,
            2018 as ano,
            null::bigint as populacao,
            mcul01 as tipo_orgao_gestor,
            {{ eixo1_sim_nao("mcul10") }} as tem_plano,
            {{ eixo1_sim_nao("mcul19") }} as tem_conselho,
            {{ eixo1_sim_nao("mcul33") }} as tem_fundo,
            {{ eixo1_sim_nao("mcul15") }} as tem_lei_patrimonio,
            {{ eixo1_sim_nao("mcul26") }} as tem_cons_patrimonio,
            {{ eixo1_sim_nao_exato("mcul362") }} as equip_biblioteca,
            {{ eixo1_sim_nao_exato("mcul361") }} as equip_museu,
            {{ eixo1_sim_nao_exato("mcul365") }} as equip_teatro,
            {{ eixo1_sim_nao_exato("mcul364") }} as equip_cinema,
            mcul03 as gestor_sexo,
            mcul04 as gestor_idade,
            mcul05 as gestor_cor_raca,
            mcul06 as gestor_escolaridade,
            case
                when mcul101a is null
                then null
                when mcul101a ~ '[0-9]'
                then 'Sim'
                else 'Não'
            end as plano_tem_lei,
            {{ eixo1_sim_nao("mcul11") }} as plano_participacao_soc,
            {{ eixo1_sim_nao("mcul1111") }} as sec_educacao,
            {{ eixo1_sim_nao("mcul1112") }} as sec_esporte,
            {{ eixo1_sim_nao("mcul1113") }} as sec_turismo,
            case
                when mcul22 ~ '^-?[0-9]+(\.[0-9]+)?$' then mcul22::numeric
            end as cons_qtd_membros,
            {{ eixo1_sim_nao("mcul231") }} as cons_cap_continuada,
            {{ eixo1_sim_nao("mcul232") }} as cons_cap_eventual,
            {{ eixo1_sim_nao("mcul233") }} as cons_cap_nao_realiza,
            null::text as orcamento_perc_executado
        from {{ ref("silver_munic_2018_cultura") }}
    ),

    -- 2014 e 2006 vêm em abas separadas do suplemento de cultura, ligadas pelo
    -- código do município (a1). A aba do órgão gestor é a base, como no R.
    munic_2014 as (
        select
            orgao.a1::text as cod_municipio,
            null::text as municipio,
            2014 as ano,
            null::bigint as populacao,
            orgao.a2 as tipo_orgao_gestor,
            {{ eixo1_sim_nao("politicas.a93") }} as tem_plano,
            {{ eixo1_sim_nao("instancias.a305") }} as tem_conselho,
            {{ eixo1_sim_nao("fundo.a372") }} as tem_fundo,
            {{ eixo1_sim_nao("legislacao.a288") }} as tem_lei_patrimonio,
            {{ eixo1_sim_nao("instancias.a339") }} as tem_cons_patrimonio,
            {{ eixo1_sim_nao_exato("equipamentos.a413") }} as equip_biblioteca,
            {{ eixo1_sim_nao_exato("equipamentos.a415") }} as equip_museu,
            {{ eixo1_sim_nao_exato("equipamentos.a417") }} as equip_teatro,
            {{ eixo1_sim_nao_exato("equipamentos.a427") }} as equip_cinema,
            rh.a22 as gestor_sexo,
            rh.a23 as gestor_idade,
            null::text as gestor_cor_raca,
            rh.a20 as gestor_escolaridade,
            null::text as plano_tem_lei,
            null::text as plano_participacao_soc,
            null::text as sec_educacao,
            null::text as sec_esporte,
            null::text as sec_turismo,
            null::numeric as cons_qtd_membros,
            null::text as cons_cap_continuada,
            null::text as cons_cap_eventual,
            null::text as cons_cap_nao_realiza,
            null::text as orcamento_perc_executado
        from {{ ref("silver_munic_cultura_2014_orgao_gestor") }} as orgao
        left join
            {{ ref("silver_munic_cultura_2014_politicas_culturais") }} as politicas
            on orgao.a1 = politicas.a1
        left join
            {{ ref("silver_munic_cultura_2014_instancias_participativas") }} as instancias
            on orgao.a1 = instancias.a1
        left join
            {{ ref("silver_munic_cultura_2014_fundo_cultura") }} as fundo
            on orgao.a1 = fundo.a1
        left join
            {{ ref("silver_munic_cultura_2014_recursos_humanos") }} as rh
            on orgao.a1 = rh.a1
        left join
            {{ ref("silver_munic_cultura_2014_legislacao") }} as legislacao
            on orgao.a1 = legislacao.a1
        left join
            {{ ref("silver_munic_cultura_2014_equipamentos") }} as equipamentos
            on orgao.a1 = equipamentos.a1
    ),

    munic_2006 as (
        select
            orgao.a1::text as cod_municipio,
            null::text as municipio,
            2006 as ano,
            null::bigint as populacao,
            orgao.a2 as tipo_orgao_gestor,
            {{ eixo1_sim_nao("instrumentos.a104") }} as tem_plano,
            {{ eixo1_sim_nao("conselhos.a130") }} as tem_conselho,
            {{ eixo1_sim_nao("fundo.a196") }} as tem_fundo,
            {{ eixo1_sim_nao("legislacao.a120") }} as tem_lei_patrimonio,
            {{ eixo1_sim_nao("conselhos.a164") }} as tem_cons_patrimonio,
            {{ eixo1_sim_nao_exato("equipamentos.a424") }} as equip_biblioteca,
            {{ eixo1_sim_nao_exato("equipamentos.a427") }} as equip_museu,
            {{ eixo1_sim_nao_exato("equipamentos.a430") }} as equip_teatro,
            {{ eixo1_sim_nao_exato("equipamentos.a439") }} as equip_cinema,
            null::text as gestor_sexo,
            null::text as gestor_idade,
            null::text as gestor_cor_raca,
            rh.a12 as gestor_escolaridade,
            null::text as plano_tem_lei,
            null::text as plano_participacao_soc,
            null::text as sec_educacao,
            null::text as sec_esporte,
            null::text as sec_turismo,
            null::numeric as cons_qtd_membros,
            null::text as cons_cap_continuada,
            null::text as cons_cap_eventual,
            null::text as cons_cap_nao_realiza,
            null::text as orcamento_perc_executado
        from {{ ref("silver_munic_cultura_2006_orgao_gestor") }} as orgao
        left join
            {{ ref("silver_munic_cultura_2006_instrumentos_de_gestao") }} as instrumentos
            on orgao.a1 = instrumentos.a1
        left join
            {{ ref("silver_munic_cultura_2006_conselhos_municipais") }} as conselhos
            on orgao.a1 = conselhos.a1
        left join
            {{ ref("silver_munic_cultura_2006_fundo_municipal") }} as fundo
            on orgao.a1 = fundo.a1
        left join
            {{ ref("silver_munic_cultura_2006_recursos_humanos") }} as rh
            on orgao.a1 = rh.a1
        left join
            {{ ref("silver_munic_cultura_2006_legislacao") }} as legislacao
            on orgao.a1 = legislacao.a1
        left join
            {{ ref("silver_munic_cultura_2006_equipamentos") }} as equipamentos
            on orgao.a1 = equipamentos.a1
    ),

    edicoes as (
        select *
        from munic_2021
        union all
        select *
        from munic_2018
        union all
        select *
        from munic_2014
        union all
        select *
        from munic_2006
    ),

    -- O código de 2006 tem 6 dígitos, sem o verificador. O R casa toda edição com o
    -- código e o nome de 2021 pelos 6 primeiros dígitos.
    municipios_2021 as (
        select distinct
            cod_municipio as cod_oficial,
            municipio as nome_oficial,
            left(cod_municipio, 6) as raiz_6_digitos
        from munic_2021
    ),

    painel as (
        select
            e.*,
            coalesce(m.cod_oficial, e.cod_municipio) as cod_municipio_oficial,
            coalesce(m.nome_oficial, e.municipio) as municipio_oficial
        from edicoes as e
        left join municipios_2021 as m on m.raiz_6_digitos = left(e.cod_municipio, 6)
    ),

    classificado as (
        select
            *,
            case
                when gestor_idade ~ '^-?[0-9]+(\.[0-9]+)?$' then gestor_idade::numeric
            end as gestor_idade_num,
            case
                when
                    gestor_cor_raca in ('Branca', 'Preta', 'Parda', 'Amarela', 'Indígena')
                then gestor_cor_raca
            end as gestor_cor_raca_limpa
        from painel
    ),

    final as (
        select
            *,
            left(cod_municipio_oficial, 2) as cod_uf,
            case
                left(cod_municipio_oficial, 2)
                when '11'
                then 'RO'
                when '12'
                then 'AC'
                when '13'
                then 'AM'
                when '14'
                then 'RR'
                when '15'
                then 'PA'
                when '16'
                then 'AP'
                when '17'
                then 'TO'
                when '21'
                then 'MA'
                when '22'
                then 'PI'
                when '23'
                then 'CE'
                when '24'
                then 'RN'
                when '25'
                then 'PB'
                when '26'
                then 'PE'
                when '27'
                then 'AL'
                when '28'
                then 'SE'
                when '29'
                then 'BA'
                when '31'
                then 'MG'
                when '32'
                then 'ES'
                when '33'
                then 'RJ'
                when '35'
                then 'SP'
                when '41'
                then 'PR'
                when '42'
                then 'SC'
                when '43'
                then 'RS'
                when '50'
                then 'MS'
                when '51'
                then 'MT'
                when '52'
                then 'GO'
                when '53'
                then 'DF'
                else 'Outro'
            end as uf,
            case
                left(cod_municipio_oficial, 1)
                when '1'
                then 'Norte'
                when '2'
                then 'Nordeste'
                when '3'
                then 'Sudeste'
                when '4'
                then 'Sul'
                when '5'
                then 'Centro-Oeste'
                else 'Sem Informação'
            end as regiao,
            case
                when gestor_idade_num < 30
                then 'Até 29 anos'
                when gestor_idade_num >= 30 and gestor_idade_num <= 39
                then '30 a 39 anos'
                when gestor_idade_num >= 40 and gestor_idade_num <= 49
                then '40 a 49 anos'
                when gestor_idade_num >= 50 and gestor_idade_num <= 59
                then '50 a 59 anos'
                when gestor_idade_num >= 60
                then '60 anos ou mais'
            end as gestor_faixa_etaria,
            case
                when gestor_cor_raca_limpa in ('Preta', 'Parda')
                then 'Negros (Pretos e Pardos)'
                when gestor_cor_raca_limpa = 'Branca'
                then 'Brancos'
                when gestor_cor_raca_limpa in ('Amarela', 'Indígena')
                then 'Amarela / Indígena'
            end as gestor_grupo_raca,
            case
                when gestor_escolaridade is null
                then null
                when gestor_escolaridade ~* 'pós|especializa|mestrado|doutorado'
                then 'Pós-Graduação'
                when gestor_escolaridade ~* 'superior|graduação'
                then 'Ensino Superior'
                when gestor_escolaridade ~* 'médio|2º grau|2° grau|2o grau|segundo grau'
                then 'Ensino Médio'
                when
                    gestor_escolaridade
                    ~* 'fundamental|1º grau|1° grau|1o grau|sem instrução|primeiro grau'
                then 'Ensino Fundamental'
                else 'Sem Informação'
            end as gestor_escolaridade_agrupada
        from classificado
    )

select
    cod_municipio_oficial as cod_municipio,
    municipio_oficial as municipio,
    cod_uf,
    uf,
    regiao,
    ano,
    populacao,
    tipo_orgao_gestor,
    tem_plano,
    tem_conselho,
    tem_fundo,
    tem_lei_patrimonio,
    tem_cons_patrimonio,
    equip_biblioteca,
    equip_museu,
    equip_teatro,
    equip_cinema,
    gestor_sexo,
    gestor_idade,
    gestor_cor_raca,
    gestor_escolaridade,
    gestor_idade_num,
    gestor_faixa_etaria,
    gestor_cor_raca_limpa,
    gestor_grupo_raca,
    gestor_escolaridade_agrupada,
    plano_tem_lei,
    plano_participacao_soc,
    sec_educacao,
    sec_esporte,
    sec_turismo,
    cons_qtd_membros,
    cons_cap_continuada,
    cons_cap_eventual,
    cons_cap_nao_realiza,
    orcamento_perc_executado
from final
