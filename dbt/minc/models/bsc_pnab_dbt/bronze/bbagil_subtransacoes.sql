{{ config(
    materialized='incremental',
    unique_key=['ente', 'id_transacao_pai', 'id']
) }}

SELECT
    ente,
    id_transacao_pai,
    id::bigint AS id,
    CASE
        WHEN paymentdate IS NOT NULL AND paymentdate <> ''
        THEN TO_DATE(paymentdate, 'DD/MM/YYYY')
    END AS paymentdate,
    paymentstatus,
    value::numeric AS value,
    beneficiarydocumentid,
    beneficiaryname,
    subtransactionaccountabilityname,
    codesubtransactionstate,
    beneficiarybankidentifiercode,
    beneficiarybranchcode,
    beneficiaryaccountnumber,
    beneficiarypersontype,
    attachedexpensedocumentindicator,
    expensecategorycode,
    subtransactionaccountabilityindicator,
    bankorderpurposecode,
    bankorderrulecode,
    bankorderpurposedescription,
    expensesequentialnumber,
    expensescategory,
    expensesdocuments
FROM {{ source('bsc_pnab', 'raw_bbagil_subtransacoes') }}

{% if is_incremental() %}
WHERE (ente, id_transacao_pai, id::bigint) NOT IN (
    SELECT ente, id_transacao_pai, id FROM {{ this }}
)
{% endif %}
