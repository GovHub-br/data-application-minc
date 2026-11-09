-- Gold do Eixo 1, frente ESTADIC: uma linha por UF em 2021 com as três
-- categorias dos mapas do ESTADIC.R (mapas 8.1 a 8.3). Desenhar o mapa fica com
-- o consumidor.
--
-- maturidade_snc soma plano, conselho e fundo: com um deles ausente, a soma é
-- NULL e a maturidade também, como no R.
with

    uf_2021 as (
        select
            cod_uf,
            sigla_uf,
            nome_uf,
            regiao,
            tipo_orgao_gestor,
            tem_lei_patrimonio,
            tem_lei_incentivo,
            (tem_plano = 'Sim')::int
            + (tem_conselho = 'Sim')::int
            + (tem_fundo = 'Sim')::int as instrumentos_do_tripe
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano = 2021
    )

select
    cod_uf,
    sigla_uf,
    nome_uf,
    regiao,
    tipo_orgao_gestor,
    instrumentos_do_tripe,
    case
        when instrumentos_do_tripe = 3
        then 'Tripé Completo'
        when instrumentos_do_tripe in (1, 2)
        then 'Institucionalização Parcial'
        when instrumentos_do_tripe = 0
        then 'Nenhum Instrumento'
    end as maturidade_snc,
    case
        when tem_lei_patrimonio = 'Sim' and tem_lei_incentivo = 'Sim'
        then 'Possui Ambas as Leis'
        when tem_lei_patrimonio = 'Sim' and tem_lei_incentivo = 'Não'
        then 'Apenas Lei de Patrimônio'
        when tem_lei_patrimonio = 'Não' and tem_lei_incentivo = 'Sim'
        then 'Apenas Lei de Incentivo'
        else 'Não Possui/Sem Informação'
    end as perfil_fomento
from uf_2021
