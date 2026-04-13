SELECT 
    n.numero AS codigo,
    m.chave_nfe,
    m.dhEmi AS data_emissao,
    m.xNome AS fornecedor,
    m.CNPJCPF AS cpnj_cpf,
    CAST(REPLACE(m.vNF, ',', '.') AS DECIMAL(15,2)) AS valor,
    n.natureza_operacao,
    FALSE AS processed,
    
    -- Junta as colunas da tabela n (vêm NULL se a nota só estiver no monitor)
    TIMESTAMP(n.data_edicao, n.hora_chegada) AS data_hora_entrada,
    
	CASE 
        WHEN n.chave IS NULL THEN 'Pendente Importação'
        WHEN n.movimentacao_estoque = 1 AND n.status_alt_preco = 1 THEN 'Processado Total'
        WHEN n.movimentacao_estoque = 1 AND n.status_alt_preco = 0 THEN 'Estoque Atualizado (Preço Pendente)'
        ELSE 'Em Processamento'
    END AS status_processamento,
    MAX(m.deuCiencia) AS manifestacao,
    
    -- Define o status do XML
    IF(SUM(m.schemaType = 'procNFe') > 0, 'XML Disponível', 'Apenas Resumo') AS status_xml

FROM dfe_server_log_monitor_notas m
LEFT JOIN `gtech-gestao`.notas_entrada n
    ON n.chave = m.chave_nfe 
    AND n.movimentacao_estoque = 1

WHERE 
    -- Filtro dinâmico para o mês atual
    STR_TO_DATE(m.dhEmi, '%d/%m/%Y %H:%i:%s') >= DATE_FORMAT(NOW(), '%Y-%m-01 00:00:00')
    AND STR_TO_DATE(m.dhEmi, '%d/%m/%Y %H:%i:%s') <= LAST_DAY(NOW()) + INTERVAL 1 DAY - INTERVAL 1 SECOND

GROUP BY 
    m.chave_nfe,
    m.dhEmi,
    m.xNome,
    m.CNPJCPF,
    m.vNF;