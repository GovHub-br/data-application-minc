-- Nenhum lançamento que a LAB 1 municipal contaria sai da conta por falta de
-- data. Quando o bookingdate não é D/M/AAAA, a macro data_texto devolve nulo, o
-- exercício e o IPCA ficam nulos, e a gold-base descarta a linha no filtro do
-- ano fechado (ipca_meses_ano = 12), junto com as do ano aberto, sem aviso.
--
-- Lê a intermediate, antes desse descarte: os débitos do programa 30882120200002
-- (indicador diferente de 'C', o nulo incluído) fora os movimentos de aplicação
-- 248, 271 e 345, que a gold-base tira de qualquer jeito. É a mesma regra da
-- gold-base, menos o filtro do ano. Aviso, e não erro: a linha some da Fig. 7,
-- mas o resto do build continua.
--
-- Sem CTE materializada, ao contrário da gold-base: aqui todos os filtros são
-- de colunas do extrato e descem abaixo dos joins com o ente e o IPCA, e o
-- esperado é não sobrar linha nenhuma, então o join por nested loop que o
-- Postgres escolhe não custa nada. Devolve os lançamentos sem data.
{{ config(severity="warn") }}

select id_plano_acao, id_plano_acao_dado_bancario, id_transacao
from {{ ref("int_bacen_bbagil_siconfi") }}
where
    base_origem = 'bbagil_extrato'
    and codigo_programa = 30882120200002
    and creditdebitindicator is distinct from 'C'
    and (descriptioncode is null or descriptioncode not in (248, 271, 345))
    and data_lancamento is null
