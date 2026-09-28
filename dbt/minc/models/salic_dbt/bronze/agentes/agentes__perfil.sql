-- Bronze SALIC — agentes__perfil.
-- Origem: salic_bronze.agentes__perfil, onde tudo chega como texto da ingestão
-- via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Liga o agente (idagente) a um perfil e característica de acesso, codificados
-- como inteiros de bitmask da origem — ver a ressalva no schema.yml.
select
    {{ bronze_inteiro("idperfil") }} as idperfil,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("perfil") }} as perfil,
    {{ bronze_inteiro("caracteristica") }} as caracteristica,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("bronze_agentes", "agentes__perfil") }}
