SELECT 
    '1' AS filial,
    CASE ec.nome
        WHEN 'CAIXA001' THEN 1
        WHEN 'CAIXA002' THEN 2
        WHEN 'CAIXA003' THEN 3
        ELSE 0
    END AS pdv,
    evc.id_ecf_movimento,
    evc.ccf,
    evc.data_venda AS emissao,
    evc.hora_venda AS hora,
    evc.valor_venda AS v_bruto,
    evc.desconto,
    evc.acrescimo,
    evc.valor_final AS v_liquida,
    CASE WHEN evc.cupom_cancelado = 'S' THEN 1 ELSE 0 END AS canc,
    evc.nome_cliente AS cliente,
    evc.cpf_cnpj_cliente AS cnpj_cpf,
    evc.status_nfce,
    evc.valor_recebido,
    CONCAT(IFNULL(evc.serie_nfce, ''), '/', IFNULL(evc.numero_nfce, '')) AS `serie/numero`,
    evc.troco,
    emp.descricao AS tipo_pagamento_principal,
    ettp_final.maior_valor

FROM ecf_venda_cabecalho evc
INNER JOIN ecf_caixa ec 
    ON ec.id = evc.id_ecf_caixa

-- Bloco para encontrar o finalizador predominante
LEFT JOIN (
    -- Passo 3: Pegamos o tipo de pagamento que bate com o maior valor
    SELECT 
        v1.id_ecf_venda_cabecalho, 
        v1.id_ecf_caixa, 
        v1.id_ecf_tipo_pagamento,
        v2.max_v as maior_valor
    FROM (
        -- Passo 1: Soma por tipo de pagamento
        SELECT id_ecf_venda_cabecalho, id_ecf_caixa, id_ecf_tipo_pagamento, SUM(valor) as soma_tipo
        FROM ecf_total_tipo_pgto
        GROUP BY 1, 2, 3
    ) v1
    INNER JOIN (
        -- Passo 2: Descobre o maior valor somado da venda
        SELECT id_ecf_venda_cabecalho, id_ecf_caixa, MAX(soma_tipo) as max_v
        FROM (
            SELECT id_ecf_venda_cabecalho, id_ecf_caixa, id_ecf_tipo_pagamento, SUM(valor) as soma_tipo
            FROM ecf_total_tipo_pgto
            GROUP BY 1, 2, 3
        ) sub
        GROUP BY 1, 2
    ) v2 ON v1.id_ecf_venda_cabecalho = v2.id_ecf_venda_cabecalho 
        AND v1.id_ecf_caixa = v2.id_ecf_caixa 
        AND v1.soma_tipo = v2.max_v
    GROUP BY v1.id_ecf_venda_cabecalho, v1.id_ecf_caixa -- Garante uma linha se houver empate
) ettp_final ON ettp_final.id_ecf_venda_cabecalho = evc.id 
            AND ettp_final.id_ecf_caixa = evc.id_ecf_caixa

LEFT JOIN ecf_tipo_pagamento emp 
    ON emp.id = ettp_final.id_ecf_tipo_pagamento
WHERE evc.data_venda = %(data)s;