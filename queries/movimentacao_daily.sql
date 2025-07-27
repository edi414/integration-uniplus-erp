SELECT 
    data_movimentacao, 
    cod_filial, 
    cod_produto, 
    tipo_movimento, 
    quantidade, 
    valor_unitario, 
    valor_total 
FROM estoque.movimentacoes 
WHERE data_movimentacao = %(data)s 