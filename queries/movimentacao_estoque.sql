SELECT 
    'Geral' AS local_estoque,
    1 AS filial,
    CAST(c.NUMERO AS CHAR) AS documento,
    p.CODIGO as codigo,
    p.NOME as nome,
    c.DATA_EMISSAO AS datahora,
    c.ID AS currenttimemillis,
    c.TIPO_DOC AS tipodocumento,
    d.QUANTIDADE AS qtd,
    d.TIPO_MOVIMENTO AS tipo_movimentacao,
    d.VALOR_TOTAL_ITEM AS valortotal,
    0 AS precoultimacompra,
    0 AS custoaquisicao,
    0 AS customedio,
    d.VALOR_ICMS AS icms,
    d.VALOR_ICMS_ST AS icms_st,
    0 AS ippt,
    (IFNULL(d.BASE_PIS, 0) + IFNULL(d.BASE_COFINS, 0)) AS pis_cofins,
    d.VALOR_IPI_ITEM AS ipi,
    0 AS outros_impostos,
    0 AS comissao,
    d.CFOP AS cfop,
    d.UNIDADE AS un
FROM estoque_mov_det d
INNER JOIN estoque_mov_cab c ON d.ESTOQUE_MOV_CAB_ID = c.ID
INNER JOIN produto p ON d.PRODUTO_ID = p.ID
WHERE DATE(c.DATA_EMISSAO) = %(data)s;
