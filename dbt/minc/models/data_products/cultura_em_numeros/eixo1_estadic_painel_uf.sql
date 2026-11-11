-- Gold-base do Eixo 1, frente ESTADIC: o painel da gestão estadual de cultura,
-- uma linha por UF e edição da ESTADIC (2012, 2014, 2018, 2021, 2023, 2024).
--
-- Reproduz o df_estadic_painel do ESTADIC.R (seções 3 e 4), que grava
-- estadic_cultura_painel_transversal_12_24.csv. Cada edição responde a uma parte
-- do painel: 2018 e 2021, a aba Cultura; 2014, o suplemento de cultura; 2012, a
-- aba Conselhos e fundos; 2023, Primeira Infância e Política para Mulheres; 2024,
-- Igualdade Racial. A regra de negócio está aqui (macro eixo1_sim_nao, a
-- composição e a competência do conselho, os grupos de órgão gestor e de
-- escolaridade, e o tripé). As silvers do ibge só tipam.
--
-- Diferença do R: resposta ausente fica NULL (ADR 0010, regra transversal 1).
-- O R classificava o órgão gestor ausente como 'Setor Subordinado / Outros' e a
-- escolaridade ausente como 'Sem Informação' nas edições que não perguntam isso
-- (2012, 2023 e 2024).
--
-- Diferença do R: três perguntas vêm de outra variável. O ESTADIC.R lê, para o
-- plano de 2014, a a93 ("Consulta pública", subpergunta do plano); para o
-- conselho de 2014, a a305 ("Institutos Históricos e Geográficos locais"); e
-- para o fundo de 2012, a a94 ("Deliberativo", do conselho). Aqui são a a86
-- ("Plano Estadual de Cultura - existência"), a a254 ("Conselho Estadual de
-- Cultural - existência") e a a98 ("Fundo estadual de cultura - existência"),
-- como diz o dicionário do IBGE de cada edição.
with

    cultura_2021_2018 as (
        select
            cod_uf::text as cod_uf,
            2021 as ano,
            ecul01 as tipo_orgao_gestor,
            ecul10 as plano,
            ecul19 as conselho,
            ecul33 as fundo,
            ecul15 as lei_patrimonio,
            ecul26 as cons_patrimonio,
            ecul31 as lei_incentivo,
            ecul03 as gestor_sexo,
            ecul05 as gestor_cor_raca,
            ecul06 as gestor_escolaridade,
            ecul192 as composicao,
            ecul201 as consultivo,
            ecul202 as deliberativo,
            ecul203 as normativo,
            ecul204 as fiscalizador
        from {{ ref("silver_estadic_2021_cultura") }}
        union all
        select
            cod_uf::text as cod_uf,
            2018 as ano,
            ecul01 as tipo_orgao_gestor,
            ecul10 as plano,
            ecul19 as conselho,
            ecul33 as fundo,
            ecul15 as lei_patrimonio,
            ecul26 as cons_patrimonio,
            ecul31 as lei_incentivo,
            ecul03 as gestor_sexo,
            ecul05 as gestor_cor_raca,
            ecul06 as gestor_escolaridade,
            ecul192 as composicao,
            ecul201 as consultivo,
            ecul202 as deliberativo,
            ecul203 as normativo,
            ecul204 as fiscalizador
        from {{ ref("silver_estadic_2018_cultura") }}
    ),

    estadic_2021_2018 as (
        select
            cod_uf,
            ano,
            tipo_orgao_gestor,
            {{ eixo1_sim_nao("plano") }} as tem_plano,
            {{ eixo1_sim_nao("conselho") }} as tem_conselho,
            {{ eixo1_sim_nao("fundo") }} as tem_fundo,
            {{ eixo1_sim_nao("lei_patrimonio") }} as tem_lei_patrimonio,
            {{ eixo1_sim_nao("cons_patrimonio") }} as tem_cons_patrimonio,
            {{ eixo1_sim_nao("lei_incentivo") }} as tem_lei_incentivo,
            gestor_sexo,
            gestor_cor_raca,
            gestor_escolaridade,
            -- "Não paritário" é testado antes de "paritário", como na MUNIC: o R
            -- procura "paritá" primeiro e classificaria um "Não paritário" como
            -- 'Paritário'.
            case
                when composicao ~* 'n[ãÃ]o ([éÉ] )?parit'
                then 'Não Paritário'
                when composicao ~* 'paritá'
                then 'Paritário'
                when composicao ~* 'representação|governamental|sociedade civil'
                then 'Não Paritário'
                else 'Sem Informação'
            end as cons_paritario,
            case
                when {{ eixo1_sim_nao("deliberativo") }} = 'Sim'
                then 'Deliberativo'
                when {{ eixo1_sim_nao("consultivo") }} = 'Sim'
                then 'Consultivo'
                when
                    {{ eixo1_sim_nao("normativo") }} = 'Sim'
                    or {{ eixo1_sim_nao("fiscalizador") }} = 'Sim'
                then 'Normativo / Fiscalizador'
                else 'Sem Informação'
            end as cons_competencia,
            null::text as pi_arte_cultura,
            null::text as pm_parceria_cultura,
            null::text as ir_gt_cultura,
            null::text as ir_patrimonio_afro,
            null::text as ir_patrimonio_quilombola
        from cultura_2021_2018
    ),

    -- O suplemento de cultura de 2014 chama o código da UF de a1 em umas abas e de
    -- codigouf em outras; o R renomeia os dois para cod_uf.
    estadic_2014 as (
        select
            orgao.a1::text as cod_uf,
            2014 as ano,
            orgao.a2 as tipo_orgao_gestor,
            -- a86 e a254, não a a93 e a a305 do R: ver o cabeçalho.
            {{ eixo1_sim_nao("politicas.a86") }} as tem_plano,
            {{ eixo1_sim_nao("instancias.a254") }} as tem_conselho,
            {{ eixo1_sim_nao("fundo.a321") }} as tem_fundo,
            {{ eixo1_sim_nao("legislacao.a237") }} as tem_lei_patrimonio,
            null::text as tem_cons_patrimonio,
            null::text as tem_lei_incentivo,
            rh.a22 as gestor_sexo,
            null::text as gestor_cor_raca,
            rh.a20 as gestor_escolaridade,
            null::text as cons_paritario,
            null::text as cons_competencia,
            null::text as pi_arte_cultura,
            null::text as pm_parceria_cultura,
            null::text as ir_gt_cultura,
            null::text as ir_patrimonio_afro,
            null::text as ir_patrimonio_quilombola
        from {{ ref("silver_estadic_cultura_2014_orgao_gestor") }} as orgao
        left join
            {{ ref("silver_estadic_cultura_2014_politicas_culturais") }} as politicas
            on orgao.a1 = politicas.a1
        left join
            {{ ref("silver_estadic_cultura_2014_instancias_participativas") }}
            as instancias
            on orgao.a1 = instancias.codigouf
        left join
            {{ ref("silver_estadic_cultura_2014_fundo_cultura") }} as fundo
            on orgao.a1 = fundo.codigouf
        left join
            {{ ref("silver_estadic_cultura_2014_legislacao") }} as legislacao
            on orgao.a1 = legislacao.codigouf
        left join
            {{ ref("silver_estadic_cultura_2014_recursos_humanos") }} as rh
            on orgao.a1 = rh.a1
    ),

    estadic_2012 as (
        select
            a1::text as cod_uf,
            2012 as ano,
            null::text as tipo_orgao_gestor,
            null::text as tem_plano,
            {{ eixo1_sim_nao("a90") }} as tem_conselho,
            -- a98, não a a94 do R: ver o cabeçalho.
            {{ eixo1_sim_nao("a98") }} as tem_fundo,
            null::text as tem_lei_patrimonio,
            null::text as tem_cons_patrimonio,
            null::text as tem_lei_incentivo,
            null::text as gestor_sexo,
            null::text as gestor_cor_raca,
            null::text as gestor_escolaridade,
            null::text as cons_paritario,
            null::text as cons_competencia,
            null::text as pi_arte_cultura,
            null::text as pm_parceria_cultura,
            null::text as ir_gt_cultura,
            null::text as ir_patrimonio_afro,
            null::text as ir_patrimonio_quilombola
        from {{ ref("silver_estadic_2012_conselhos_e_fundos") }}
    ),

    estadic_2023 as (
        select
            infancia.cod_uf::text as cod_uf,
            2023 as ano,
            null::text as tipo_orgao_gestor,
            null::text as tem_plano,
            null::text as tem_conselho,
            null::text as tem_fundo,
            null::text as tem_lei_patrimonio,
            null::text as tem_cons_patrimonio,
            null::text as tem_lei_incentivo,
            null::text as gestor_sexo,
            null::text as gestor_cor_raca,
            null::text as gestor_escolaridade,
            null::text as cons_paritario,
            null::text as cons_competencia,
            {{ eixo1_sim_nao("infancia.epri0610") }} as pi_arte_cultura,
            {{ eixo1_sim_nao("mulheres.eppm134") }} as pm_parceria_cultura,
            null::text as ir_gt_cultura,
            null::text as ir_patrimonio_afro,
            null::text as ir_patrimonio_quilombola
        from {{ ref("silver_estadic_2023_primeira_infancia") }} as infancia
        left join
            {{ ref("silver_estadic_2023_politica_para_mulheres") }} as mulheres
            on infancia.cod_uf = mulheres.cod_uf
    ),

    estadic_2024 as (
        select
            cod_uf::text as cod_uf,
            2024 as ano,
            null::text as tipo_orgao_gestor,
            null::text as tem_plano,
            null::text as tem_conselho,
            null::text as tem_fundo,
            null::text as tem_lei_patrimonio,
            null::text as tem_cons_patrimonio,
            null::text as tem_lei_incentivo,
            null::text as gestor_sexo,
            null::text as gestor_cor_raca,
            null::text as gestor_escolaridade,
            null::text as cons_paritario,
            null::text as cons_competencia,
            null::text as pi_arte_cultura,
            null::text as pm_parceria_cultura,
            {{ eixo1_sim_nao("eigr123") }} as ir_gt_cultura,
            {{ eixo1_sim_nao("eigr128") }} as ir_patrimonio_afro,
            {{ eixo1_sim_nao("eigr1210") }} as ir_patrimonio_quilombola
        from {{ ref("silver_estadic_2024_igualdade_racial") }}
    ),

    edicoes as (
        select *
        from estadic_2021_2018
        union all
        select *
        from estadic_2014
        union all
        select *
        from estadic_2012
        union all
        select *
        from estadic_2023
        union all
        select *
        from estadic_2024
    ),

    classificado as (
        select
            *,
            case
                cod_uf
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
            end as sigla_uf,
            case
                left(cod_uf, 1)
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
                when tipo_orgao_gestor is null
                then null
                when tipo_orgao_gestor ~* 'exclusiva|fundação|indireta'
                then 'Secretaria Exclusiva / Fundação'
                when tipo_orgao_gestor ~* 'conjunto'
                then 'Secretaria Conjunta'
                else 'Setor Subordinado / Outros'
            end as tipo_orgao_gestor_agrupado,
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
            end as gestor_escolaridade_agrupada,
            -- O tripé do SNC, calculado aqui uma vez para o mapa 8.1, e só nas
            -- edições que perguntam os três instrumentos (2014, 2018 e 2021).
            -- instrumentos_do_tripe conta os 'Sim' e fica NULL com algum vazio.
            -- tem_tripe_completo é 'Sim' com os três e 'Não' com algum 'Não'; com
            -- algum vazio e nenhum 'Não' não dá para saber, e fica NULL (regra
            -- transversal 1). O R passa os três pela fix_sim_nao, que faz do vazio
            -- 'Não', e contaria essa UF como sem tripé.
            case
                when ano in (2014, 2018, 2021)
                then
                    (tem_plano = 'Sim')::int
                    + (tem_conselho = 'Sim')::int
                    + (tem_fundo = 'Sim')::int
            end as instrumentos_do_tripe,
            case
                when ano not in (2014, 2018, 2021)
                then null
                when tem_plano = 'Sim' and tem_conselho = 'Sim' and tem_fundo = 'Sim'
                then 'Sim'
                when tem_plano = 'Não' or tem_conselho = 'Não' or tem_fundo = 'Não'
                then 'Não'
            end as tem_tripe_completo
        from edicoes
    ),

    final as (
        select
            *,
            case
                sigla_uf
                when 'RO'
                then 'Rondônia'
                when 'AC'
                then 'Acre'
                when 'AM'
                then 'Amazonas'
                when 'RR'
                then 'Roraima'
                when 'PA'
                then 'Pará'
                when 'AP'
                then 'Amapá'
                when 'TO'
                then 'Tocantins'
                when 'MA'
                then 'Maranhão'
                when 'PI'
                then 'Piauí'
                when 'CE'
                then 'Ceará'
                when 'RN'
                then 'Rio Grande do Norte'
                when 'PB'
                then 'Paraíba'
                when 'PE'
                then 'Pernambuco'
                when 'AL'
                then 'Alagoas'
                when 'SE'
                then 'Sergipe'
                when 'BA'
                then 'Bahia'
                when 'MG'
                then 'Minas Gerais'
                when 'ES'
                then 'Espírito Santo'
                when 'RJ'
                then 'Rio de Janeiro'
                when 'SP'
                then 'São Paulo'
                when 'PR'
                then 'Paraná'
                when 'SC'
                then 'Santa Catarina'
                when 'RS'
                then 'Rio Grande do Sul'
                when 'MS'
                then 'Mato Grosso do Sul'
                when 'MT'
                then 'Mato Grosso'
                when 'GO'
                then 'Goiás'
                when 'DF'
                then 'Distrito Federal'
                else 'Outro'
            end as nome_uf,
            -- A maturidade do mapa 8.1, a partir do tripé acima. Com algum vazio,
            -- só sai categoria quando o vazio não muda a resposta: 'Sim' e 'Não'
            -- nos outros dois é parcial de qualquer jeito.
            case
                when tem_tripe_completo = 'Sim'
                then 'Tripé Completo'
                when instrumentos_do_tripe = 0
                then 'Nenhum Instrumento'
                when
                    tem_tripe_completo = 'Não'
                    and 'Sim' in (tem_plano, tem_conselho, tem_fundo)
                then 'Institucionalização Parcial'
            end as maturidade_snc
        from classificado
    )

select
    cod_uf,
    sigla_uf,
    nome_uf,
    regiao,
    ano,
    tipo_orgao_gestor_agrupado as tipo_orgao_gestor,
    tem_plano,
    tem_conselho,
    tem_fundo,
    instrumentos_do_tripe,
    tem_tripe_completo,
    maturidade_snc,
    tem_lei_patrimonio,
    tem_cons_patrimonio,
    tem_lei_incentivo,
    gestor_sexo,
    gestor_cor_raca,
    gestor_escolaridade,
    cons_paritario,
    cons_competencia,
    pi_arte_cultura,
    pm_parceria_cultura,
    ir_gt_cultura,
    ir_patrimonio_afro,
    ir_patrimonio_quilombola,
    gestor_escolaridade_agrupada
from final
