SELECT 
    data_vigencia, 
    cod_produto, 
    cod_filial, 
    preco_venda, 
    preco_custo, 
    margem 
FROM produtos.precos 
WHERE data_vigencia = %(data)s 