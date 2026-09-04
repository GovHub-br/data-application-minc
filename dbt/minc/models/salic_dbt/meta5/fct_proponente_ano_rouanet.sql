-- Silver SALIC / Meta 5 — acesso a fomento por proponente e ano.
--
-- GRÃO: uma linha por proponente por ano em que houve pagamento no projeto ao
-- qual ele está ligado.
--
-- PRIMEIRO ACESSO É O PRIMEIRO PAGAMENTO. Decidido em 2026-09-04: não é a
-- captação, não é a aprovação, não é o cadastro. Vale registrar o que a
-- escolha significa — pagamento no SALIC é o projeto pagando terceiros, então
-- o indicador mede o ano em que o projeto do proponente começou a executar, e
-- não o ano em que ele conseguiu recurso.
--
-- POR QUE NÃO HÁ VALOR SOMADO AQUI. A ponte de proponente tem grão PRONAC x
-- agente, e um projeto pode ter mais de um agente ligado. Somar o valor pago
-- por proponente contaria o mesmo dinheiro uma vez para cada agente, e o total
-- ficaria acima do real. É a mesma decisão de rateio que está aberta para a
-- Meta 4, e não se resolve aqui.
--
-- Contagem e ano mínimo não sofrem disso: duplicar a linha não muda o menor
-- ano, e as contagens são de distintos. Por isso este fato carrega quando e
-- quantas vezes, nunca quanto.
--
-- ZONA RESTRITA POR HERANÇA. Lê a ponte de proponente, que carrega CPF/CNPJ e
-- nome. O teste de governança proíbe modelo publicável ler modelo restrito, e
-- este segue a mesma classificação.
with

pagamentos as (
    select
        {{ pronac_normalizado("nrpronac") }} as pronac,
        aaanocomprovacao as ano,
        idapicomprovacoes as id_comprovacao
    from {{ ref('sac__tbapicomprovacoes') }}
    -- ano nulo é comprovação sem ano utilizável na origem. Fica de fora do
    -- fato porque não há a que ano atribuí-la, e mantê-la criaria uma
    -- categoria "sem ano" que ninguém pediu.
    where aaanocomprovacao is not null
),

proponentes as (
    select
        pronac,
        id_agente,
        documento_proponente,
        tipo_pessoa_proponente
    from {{ ref('brg_projeto_proponente_rouanet') }}
),

proponente_ano as (
    select
        pr.id_agente,
        pr.documento_proponente,
        pr.tipo_pessoa_proponente,
        pg.ano,
        count(distinct pg.pronac) as projetos_com_pagamento,
        count(distinct pg.id_comprovacao) as comprovacoes
    from pagamentos as pg
    inner join proponentes as pr
        on pg.pronac = pr.pronac
    group by 1, 2, 3, 4
),

primeiro as (
    select
        id_agente,
        min(ano) as ano_primeiro_pagamento
    from proponente_ano
    group by 1
)

select
    pa.id_agente,
    pa.documento_proponente,
    pa.tipo_pessoa_proponente,
    pa.ano,
    pa.projetos_com_pagamento,
    pa.comprovacoes,
    pm.ano_primeiro_pagamento,
    pa.ano = pm.ano_primeiro_pagamento as eh_primeiro_ano
from proponente_ano as pa
inner join primeiro as pm
    on pa.id_agente = pm.id_agente
