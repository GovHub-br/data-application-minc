-- Gold-base do Eixo 1, frente SNC: um município por linha, na versão mais
-- recente da lista de entes do SNC no portal de dados abertos, com as variáveis
-- e as categorias que o SNC.R usa (repositório SGE cultura-em-numeros-eixo-1).
-- As golds eixo1_snc_* leem daqui; a regra mora toda neste modelo.
--
-- O R lê a aba "Municipios" de uma planilha interna. No portal, estados e
-- municípios vêm na mesma aba, com uma linha a mais que não é ente
-- ('Não cadastrado'): o universo aqui são os códigos IBGE de 7 dígitos.
--
-- Diferença do R (ADR 0010, regra transversal 1): onde a planilha não traz o
-- valor, a categoria fica NULL. O R punha 'Não' no tripé completo e
-- 'Sem Informação' na faixa de IDH; as golds de gráfico refazem o rótulo do R
-- só onde o gráfico mostra a ausência.
with

    versao_atual as (select max(versao) as versao from {{ ref("silver_snc_entes") }}),

    municipios as (
        select e.*
        from {{ ref("silver_snc_entes") }} as e
        inner join versao_atual as v on e.versao = v.versao
        where e.cod_ibge ~ '^[0-9]{7}$'
    ),

    anos as (
        select
            *,
            extract(year from data_adesao)::int as ano_adesao,
            extract(year from data_da_assinatura_da_ata_da_ultima_reuniao)::int as ano_ata
        from municipios
    )

select
    versao,
    cod_ibge as cod_municipio,
    ente_federado as municipio,
    uf,
    regiao,
    faixa_populacional,
    idh_2021,
    situacao as situacao_adesao,
    ano_adesao,
    perfil_do_orgao_gestor as perfil_orgao_gestor,
    situacao_da_lei_do_sistema_de_cultura as situacao_lei_sistema,
    situacao_da_lei_do_conselho_de_politica_cultural as situacao_conselho,
    situacao_da_lei_do_fundo_de_cultura as situacao_fundo,
    situacao_do_plano_de_cultura as situacao_plano,
    conselho_paritario,
    natureza_do_conselho as natureza_conselho,
    possui_metas,
    ano_ata as ano_ata_ultima_reuniao,
    -- Os portes 4 e 5 juntos, como na Figura 8 da publicação (seção 5.6 do R).
    case
        when faixa_populacional like '%Grande%'
        then 'Porte 4 (Grande): acima de 100 mil habitantes'
        else faixa_populacional
    end as porte_populacional,
    case
        when idh_2021 < 0.600
        then 'Baixo (< 0,600)'
        when idh_2021 < 0.700
        then 'Médio (0,600 a 0,699)'
        when idh_2021 < 0.800
        then 'Alto (0,700 a 0,799)'
        when idh_2021 >= 0.800
        then 'Muito Alto (>= 0,800)'
    end as categoria_idh,
    -- Aderiu é 'Publicado no DOU', como no R; as outras situações são trâmite
    -- ou ausência de adesão.
    situacao = 'Publicado no DOU' as adesao_publicada,
    -- Tripé completo: conselho, fundo e plano com situação 'Concluída'. NULL só
    -- quando nenhum dos três é sabidamente outra coisa e falta algum.
    case
        when
            situacao_da_lei_do_conselho_de_politica_cultural = 'Concluída'
            and situacao_da_lei_do_fundo_de_cultura = 'Concluída'
            and situacao_do_plano_de_cultura = 'Concluída'
        then 'Sim'
        when
            not (
                situacao_da_lei_do_conselho_de_politica_cultural = 'Concluída'
                and situacao_da_lei_do_fundo_de_cultura = 'Concluída'
                and situacao_do_plano_de_cultura = 'Concluída'
            )
        then 'Não'
    end as tripe_completo,
    -- Perfil dos conselhos (seção 10 do R). Os padrões são os do R sobre o texto
    -- em minúsculas; aqui com as maiúsculas escritas, porque o ~* do Postgres
    -- depende do locale para letra acentuada.
    case
        when conselho_exclusivo_de_cultura is null
        then null
        when conselho_exclusivo_de_cultura ~* '^sim\M'
        then 'Sim'
        when conselho_exclusivo_de_cultura ~ '[nN][ãaÃA][oO]'
        then 'Não'
        else 'Não informado'
    end as perfil_conselho_exclusivo,
    case
        when conselho_paritario is null
        then null
        when conselho_paritario ~* '^sim\M'
        then 'Sim'
        when conselho_paritario ~ '[nN][ãaÃA][oO]'
        then 'Não'
        else 'Não informado'
    end as perfil_conselho_paritario,
    case
        when natureza_do_conselho is null
        then null
        when natureza_do_conselho ~* 'deliberativo'
        then 'Sim'
        when natureza_do_conselho ~* 'consultivo'
        then 'Não'
        else 'Não informado'
    end as perfil_conselho_deliberativo,
    -- Atuante: assinou a ata da última reunião em 2025 ou depois. O ano de corte
    -- é o do R ("reunião entre 2025–2026").
    case
        when possui_ata_da_ultima_reuniao_do_conselho is null
        then null
        when ano_ata >= 2025
        then 'Sim'
        when ano_ata < 2025
        then 'Não'
        when possui_ata_da_ultima_reuniao_do_conselho ~ '[nN][ãaÃA][oO]'
        then 'Não'
        else 'Não informado'
    end as perfil_conselho_atuante
from anos
