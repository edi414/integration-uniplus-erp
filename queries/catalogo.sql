SELECT  
    p.codigo as sku,  
    p.ean as ean,  
    p.nome as nome,  
    p.nomeecf as nome_pdv,  
    p.preco as preco_venda,  
    p.precoultimacompra as preco_ultima_compra,
    se.quantidade as stock
FROM  
    produto p  
LEFT JOIN saldoestoque se  
    ON se.idproduto::text = p.codigo::text  
WHERE  
    p.inativo = 0;