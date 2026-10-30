{{ config(materialized="table") }}

-- Silver — um recibo de captação da Lei Rouanet por linha, com o proponente
-- que recebeu o recurso. Junta duas tabelas do próprio SALIC (o recibo e o
-- cadastro do projeto) e tipa; não decide quais recibos contam para um
-- indicador, o que é da Gold de cada produto.
--
-- LIGAÇÃO ENTRE AS TABELAS: sac__captacao tem o dinheiro e a data mas não tem
-- o proponente (``cgccpfmecena`` é o incentivador que aportou, não quem
-- recebeu). A chave para chegar nele é o PRONAC. Não use ``captacao.idprojeto``:
-- ele é 0 na maioria das linhas.
--
-- PRONAC pela macro pronac_de_ano_sequencial, a mesma do silver_sac_captacao_rouanet:
-- os dois lados do join e a reconciliação com o fato precisam da mesma chave.
-- Ano ou sequencial fora do padrão viram PRONAC nulo e o recibo sai.
--
-- FONTE: a Bronze do SALIC, que a ingestão (Trino) grava no schema salic como
-- bronze_sac_projetos e bronze_sac_captacao; não há modelo dbt bronze. O
-- proponente vem só de ``sac__projetos``, o cadastro oficial do projeto, que em
-- 19/08/2026 cobria 100% dos recibos válidos.
--
-- Até 21/10/2026 este modelo lia a ingestão v1 (schema `bronze`) e tinha o
-- espelho da API pública (``sac__tbapiprojetorouanet``) como reserva de
-- proponente. A reserva saiu na troca para a v2, que não ingere essa tabela:
-- as duas fontes concordavam em 100,00% dos 195.240 projetos que ambas
-- resolviam, e o cadastro sozinho já resolvia todos. Um recibo cujo PRONAC o
-- cadastro não resolva fica de fora pelo INNER JOIN abaixo, sem erro; o teste
-- singular salic_recibo_sem_proponente avisa quando isso acontece, e o
-- salic_recibo_sem_proponente_em_massa trava o build se a perda for grande.
--
-- JANELA: recibos de 1993 em diante — praticamente toda a vigência da lei
-- (1991).
with
    proponente_bruto as (
        select
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            case
                when length(regexp_replace(trim(cgccpf), '[^0-9]', '', 'g')) <= 11
                then lpad(regexp_replace(trim(cgccpf), '[^0-9]', '', 'g'), 11, '0')
                else lpad(regexp_replace(trim(cgccpf), '[^0-9]', '', 'g'), 14, '0')
            end as proponente_documento,
            -- o cadastro não traz o nome do proponente; a coluna fica pelo
            -- contrato do modelo
            null::text as proponente_nome
        from {{ source("salic", "bronze_sac_projetos") }}
        where
            cgccpf is not null
            and length(regexp_replace(trim(cgccpf), '[^0-9]', '', 'g')) between 8 and 14
    ),

    projeto_proponente as (
        -- uma linha por PRONAC
        select distinct on (pronac) pronac, proponente_documento, proponente_nome
        from proponente_bruto
        where pronac is not null and proponente_documento !~ '^0+$'
        order by pronac, proponente_documento
    ),

    recibos as (
        select
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            {{ bronze_timestamp("dtrecibo") }}::date as data_recibo,
            {{ bronze_numerico("captacaoreal") }} as valor_captado
        from {{ source("salic", "bronze_sac_captacao") }}
    )

select p.proponente_documento, p.proponente_nome, r.data_recibo, r.valor_captado, r.pronac
from recibos r
inner join projeto_proponente p on r.pronac = p.pronac
where r.data_recibo is not null and r.valor_captado is not null
