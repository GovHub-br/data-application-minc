-- Cada recorte do fornecedores_pagamento e parte do pagamento a PJ, e o pago a PJ
-- e parte do pago total. Uma linha aqui quer dizer que um filtro de coluna deixou
-- passar PF ou pagamento sem documento.
select pronac
from {{ ref("fornecedores_pagamento") }}
where
    valor_pago_pj > valor_pago
    or valor_pago_pj_sem_base > valor_pago_pj
    or valor_pago_a_empresa_irregular_na_data > valor_pago_pj
    or valor_pago_a_optante_simples_atual > valor_pago_pj
    or qt_pagamentos_a_empresa_irregular_na_data > qt_pagamentos_pj
    or qt_pagamentos_antes_da_abertura > qt_pagamentos_pj
    or qt_pagamentos_documento_invalido_pj > qt_pagamentos_pj
    or qt_pagamentos_pj + qt_pagamentos_sem_documento > qt_pagamentos
