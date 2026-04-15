SELECT
    p.id AS id_origem,
    'P' AS tipo,
    SUBSTRING_INDEX(c.documento, '/', 1) AS documento,
    f.RAZAO_SOCIAL AS razao_social,
    LEFT(p.status, 1) AS status,
    p.parcelas AS parcela,
    p.valor AS valor,
    p.valor_pagar AS saldo,
    c.data_emissao AS emissao,
    p.data_vencimento AS vencimento_original,
    p.data_vencimento AS vencimento,
    c.entrada AS entrada,
    p.data_pagamento AS pagamento,
    p.data_pagamento AS baixa,
    c.data_emissao AS registro,
    c.referente AS historico,
    NULL AS codigo_barras,
    NULL AS codigo_digitado
FROM parcelas_pagar p
INNER JOIN contas_pagar c ON p.id_contas_pagar = c.id
LEFT JOIN fornecedor f ON c.id_fornecedor = f.id
WHERE p.excluido = 'N';
