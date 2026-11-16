-- Gold — Meta 2 do PNAB: o repasse planejado nos planos de ação, somado por ciclo,
-- porte, UF e situação do plano. Lê só eixo2_meta2_plano_acao e não filtra nada:
-- a soma desta tabela é a soma da gold-base, que é a da silver nos cinco
-- programas (teste eixo2_meta2_repasse_planejado_soma_igual_silver).
--
-- É o repasse PLANEJADO pela União ao ente, não o executado: o que o ente pagou a
-- agentes culturais não está aqui. Cada situação do plano fica numa linha própria,
-- porque quais situações contam ainda não foi decidido.
select
    ano_programa,
    porte_ente,
    sigla_uf,
    cod_ibge_uf,
    situacao_plano_acao,
    count(*) as quantidade_planos,
    sum(valor_total_repasse_plano_acao) as valor_total_repasse_planejado
from {{ ref("eixo2_meta2_plano_acao") }}
group by ano_programa, porte_ente, sigla_uf, cod_ibge_uf, situacao_plano_acao
