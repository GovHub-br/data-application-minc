{{ config(
    materialized='incremental',
    unique_key=['ente', 'id']
) }}

SELECT
    ente,
    id::bigint AS id,
    id_programa,
    codigo_programa,
    nome_programa,
    periodo_inicial::date AS periodo_inicial,
    periodo_final::date AS periodo_final,
    CASE
        WHEN valuedate IS NOT NULL AND valuedate <> ''
        THEN TO_DATE(valuedate, 'DD/MM/YYYY')
    END AS valuedate,
    value::numeric AS value,
    descriptionname,
    creditdebitindicator,
    beneficiarydocumentid,
    beneficiaryname,
    subtransactionquantity::int AS subtransactionquantity,
    bookingdate,
    orderindex,
    referencenumber,
    accountbalance,
    descriptioncode,
    descriptionbatchnumber,
    beneficiarybankidentifiercode,
    beneficiarybranchcode,
    beneficiaryaccountnumber,
    beneficiarypersontype,
    pendingexpenseconciliation,
    attachedexpensedocumentindicator,
    expensecategorycode,
    expenseidentificationstatus,
    bankorderpurposecode,
    bankorderrulecode,
    bankorderpurposedescription,
    expensescategory,
    expensesdocuments
FROM {{ source('bsc_pnab', 'raw_bbagil_extrato_transacoes') }}

{% if is_incremental() %}
WHERE (ente, id::bigint) NOT IN (
    SELECT ente, id FROM {{ this }}
)
{% endif %}
