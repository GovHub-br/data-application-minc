-- Silver dados_cultura — snc_entes.
-- Origem: bronze_snc_entes, cópia fiel de seeds.dados_cultura_snc_entes, onde
-- tudo chega como text da carga de fonte estática (dados_cultura_dag, ADR 0012).
-- Uma linha por ente e versão do arquivo publicado. Tipar é o trabalho desta
-- camada; qual versão vale e quem é município é regra da gold.
--
-- 48 das 58 colunas: ficam de fora as 10 de cliente_dados_cultura.DADOS_SENSIVEIS
-- (dados bancários, CNPJ, nome do prefeito e do gestor, endereço, bairro e CEP).
-- 12 tipadas pelo padrão medido no dado da versão de 28/05/2026, com 100%
-- dos valores preenchidos casando ou o resto sendo texto de ausência ('Não
-- informado', 'Nao possui adesão', 'Não cadastrado'), que vira NULL: datas
-- 'dd/mm/aaaa' pela data_texto, PIB e IDH numeric, população bigint, último ano
-- de vigência do plano integer. cod_ibge fica texto: é a chave, e a linha que
-- não é ente traz 'Não cadastrado' nela. ultima_atualizacao fica texto
-- ('dd/mm/aaaa às hh:mm:ss').
select
    {{ bronze_texto("ente_federado") }} as ente_federado,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("cod_ibge") }} as cod_ibge,
    {{ bronze_numerico("pib_2016") }} as pib_2016,
    {{ bronze_numerico("idh_2021") }} as idh_2021,
    {{ bronze_inteiro("populacao_2022", "bigint") }} as populacao_2022,
    {{ bronze_texto("faixa_populacional") }} as faixa_populacional,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("situacao_da_lei_do_sistema_de_cultura") }}
    as situacao_da_lei_do_sistema_de_cultura,
    {{ data_texto("data_da_lei_do_sistema_de_cultura") }}
    as data_da_lei_do_sistema_de_cultura,
    {{ data_texto("data_adesao") }} as data_adesao,
    {{ bronze_texto("situacao_do_orgao_gestor") }} as situacao_do_orgao_gestor,
    {{ data_texto("data_da_lei_do_orgao_gestor") }} as data_da_lei_do_orgao_gestor,
    {{ bronze_texto("perfil_do_orgao_gestor") }} as perfil_do_orgao_gestor,
    {{ bronze_texto("orgao_gestor_de_cultura_possui_cnpj") }}
    as orgao_gestor_de_cultura_possui_cnpj,
    {{ bronze_texto("situacao_do_comprovante_do_cnpj_do_orgao_gestor_de_cultura") }}
    as situacao_do_comprovante_do_cnpj_do_orgao_gestor_de_cultura,
    {{ bronze_texto("situacao_da_ata_do_conselho_de_politica_cultural") }}
    as situacao_da_ata_do_conselho_de_politica_cultural,
    {{ bronze_texto("situacao_da_lei_do_conselho_de_politica_cultural") }}
    as situacao_da_lei_do_conselho_de_politica_cultural,
    {{ bronze_texto("conselho_de_cultura_possui_mesma_lei_do_sistema_municipal_de_cu") }}
    as conselho_de_cultura_possui_mesma_lei_do_sistema_municipal_de_cu,
    {{ data_texto("data_da_lei_do_conselho_de_politica_cultural") }}
    as data_da_lei_do_conselho_de_politica_cultural,
    {{ bronze_texto("possui_ata_da_ultima_reuniao_do_conselho") }}
    as possui_ata_da_ultima_reuniao_do_conselho,
    {{ data_texto("data_da_assinatura_da_ata_da_ultima_reuniao") }}
    as data_da_assinatura_da_ata_da_ultima_reuniao,
    {{ bronze_texto("conselho_exclusivo_de_cultura") }} as conselho_exclusivo_de_cultura,
    {{ bronze_texto("conselho_paritario") }} as conselho_paritario,
    {{ bronze_texto("natureza_do_conselho") }} as natureza_do_conselho,
    {{ bronze_texto("fundo_de_cultura_possui_mesma_lei_do_sistema_municipal_de_cultu") }}
    as fundo_de_cultura_possui_mesma_lei_do_sistema_municipal_de_cultu,
    {{ bronze_texto("fundo_de_cultura_possui_cnpj") }} as fundo_de_cultura_possui_cnpj,
    {{ bronze_texto("situacao_do_comprovante_do_cnpj_do_fundo_de_cultura") }}
    as situacao_do_comprovante_do_cnpj_do_fundo_de_cultura,
    {{ bronze_texto("situacao_da_lei_do_fundo_de_cultura") }}
    as situacao_da_lei_do_fundo_de_cultura,
    {{ data_texto("data_da_lei_do_fundo_de_cultura") }}
    as data_da_lei_do_fundo_de_cultura,
    {{ bronze_texto("situacao_do_plano_de_cultura") }} as situacao_do_plano_de_cultura,
    {{ data_texto("data_do_plano_de_cultura") }} as data_do_plano_de_cultura,
    {{ bronze_texto("plano_exclusivo_de_cultura") }} as plano_exclusivo_de_cultura,
    {{ bronze_texto("lei_do_plano_de_cultura_possui_anexo") }}
    as lei_do_plano_de_cultura_possui_anexo,
    {{ bronze_texto("anexo_consta_no_mesmo_arquivo_da_lei") }}
    as anexo_consta_no_mesmo_arquivo_da_lei,
    {{ bronze_inteiro("ultimo_ano_de_vigencia_do_plano_de_cultura") }}
    as ultimo_ano_de_vigencia_do_plano_de_cultura,
    {{ bronze_texto("periocidade_do_plano_de_cultura") }}
    as periocidade_do_plano_de_cultura,
    {{ bronze_texto("possui_metas") }} as possui_metas,
    {{ bronze_texto("situacao_das_metas") }} as situacao_das_metas,
    {{ bronze_texto("plano_monitorado") }} as plano_monitorado,
    {{ bronze_texto("participou_de_algum_programa_de_formacao_de_gestores_e_conselhe") }}
    as participou_de_algum_programa_de_formacao_de_gestores_e_conselhe,
    {{ bronze_texto("participou_da_conferencia_nacional") }}
    as participou_da_conferencia_nacional,
    {{ bronze_texto("oficio_indicacao_cadastrador") }} as oficio_indicacao_cadastrador,
    {{ bronze_texto("num_processo_sei") }} as num_processo_sei,
    {{ bronze_texto("ultima_atualizacao") }} as ultima_atualizacao,
    {{ bronze_data("versao") }} as versao,
    {{ bronze_texto("dt_ingest") }} as dt_ingest
from {{ ref("bronze_snc_entes") }}
