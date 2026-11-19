-- Gold / analise_rouanet -- concentracao da captacao por incentivador em cada projeto.
--
-- GRAO: 1 linha por PRONAC. UNIVERSO: os PRONACs de `silver_sac_captacao_rouanet` com
-- ao menos um incentivador de saldo positivo.
--
-- POR QUE AGREGADO, E NAO O RECIBO COM PSEUDONIMO. O pedido original era o recibo
-- com um id de incentivador por HMAC. Isso nao cabe hoje: o segredo gerenciado que
-- o plano exige nao existe (ver `silver_sac_projeto_proponente_rouanet`), e o plano
-- classifica a chave HMAC como dado linkavel e sensivel, o que faria o fato ser
-- restrito. Este agregado responde a concentracao dentro do projeto; o padrao do
-- mesmo incentivador entre projetos continua sem resposta ate essa decisao.
--
-- O DOCUMENTO SO E LIDO, NUNCA SAI. `cgccpfmecena` de `silver_sac_captacao` serve
-- para agrupar os recibos de um mesmo incentivador e nao entra no select final.
-- Todo recibo tem documento de 11 ou 14 digitos (358.844 CPF, 309.921 CNPJ).
--
-- ESTORNO. Os recibos de um incentivador sao somados com o estorno descontado; quem
-- fica com saldo zero ou negativo nao conta como incentivador nem entra no total.
-- O total de referencia das partes e a soma dos saldos positivos, que pode diferir
-- da soma bruta da captacao do projeto.
with

    recibo as (
        select
            c.pronac,
            {{ normaliza_documento("s.cgccpfmecena") }} as incentivador,
            c.valor_captado_reais
        from {{ ref("silver_sac_captacao_rouanet") }} as c
        inner join {{ ref("silver_sac_captacao") }} as s on c.id_captacao = s.idcaptacao
    ),

    saldo as (
        select pronac, incentivador, sum(valor_captado_reais) as valor
        from recibo
        group by pronac, incentivador
    ),

    parte as (
        select
            pronac,
            valor / sum(valor) over (partition by pronac) as parte,
            row_number() over (partition by pronac order by valor desc) as posicao
        from saldo
        where valor > 0
    )

select
    pronac,
    count(*) as qt_incentivadores,
    max(parte) as parte_do_maior_incentivador,
    sum(parte) filter (where posicao <= 3) as parte_dos_3_maiores,
    sum(parte * parte) as indice_hhi
from parte
group by pronac
