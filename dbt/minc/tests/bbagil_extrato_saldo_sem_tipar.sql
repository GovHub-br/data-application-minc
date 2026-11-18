{{ config(severity="warn") }}

-- Saldo que o extrato traz e a silver_extrato_bbagil não conseguiu tipar.
-- bronze_numerico só aceita decimal com ponto: um formato novo na origem (vírgula,
-- notação científica) viraria NULL sem aviso, e a coluna não tem outro teste.
--
-- Compara contagens em vez de juntar as duas tabelas: a silver lê a bronze sem
-- filtro, uma linha para cada linha, e todo saldo não nulo na silver vem de um
-- saldo preenchido na bronze. Contagens iguais, portanto, querem dizer que nenhum
-- saldo se perdeu. Fora da conta ficam o NULL e o texto 'NaN', que são a mesma
-- falta de saldo (o 'NaN' é o que o ClientPostgresDB gravava até o PR #112).
--
-- warn, e não error: nenhum modelo lê accountbalance, e um erro aqui pararia a
-- cadeia dos pagamentos da Meta 5, que lê esta silver.
with
    bronze as (
        select count(*) as saldos
        from {{ ref("bronze_extrato_bbagil") }}
        where
            nullif(trim(accountbalance), '') is not null and trim(accountbalance) <> 'NaN'
    ),

    silver as (
        select count(accountbalance) as saldos from {{ ref("silver_extrato_bbagil") }}
    )

select bronze.saldos as saldos_na_bronze, silver.saldos as saldos_na_silver
from bronze
cross join silver
where bronze.saldos <> silver.saldos
