SELECT 
    'Geral' AS local_estoque,
    1 AS filial,
    CAST(CONCAT(COALESCE(c.NUMERO_NFCE, c.COO, c.ID), '-', c.ID_ECF_CAIXA) AS CHAR) AS documento,
    c.ID as id_documento,
    CAST(p.ID AS CHAR) AS codigo,
    p.DESCRICAOPDV AS nome,
    CAST(CONCAT(c.DATA_VENDA, ' ', COALESCE(c.HORA_VENDA, '00:00:00')) AS DATETIME) AS datahora,
    (c.ID_ECF_CAIXA * 100000000 + d.ID) AS currenttimemillis,
    65 AS tipodocumento,
    d.QUANTIDADE AS qtd,
    IF(c.STATUS_VENDA = 'C' OR d.CANCELADO = 'S' OR c.CUPOM_CANCELADO = 'S', 'C', 'S') AS tipo_movimentacao,
    d.TOTAL_FINAL AS valortotal,
    IFNULL(p.VALOR_COMPRA, 0) AS precoultimacompra,
    IFNULL(d.VALOR_CUSTO_UNITARIO, 0) AS custoaquisicao,
    IFNULL(d.VALOR_CUSTO_UNITARIO, 0) AS customedio,
    IFNULL(d.ICMS, 0) AS icms,
    0 AS icms_st,
    IFNULL(p.IPPT, 0) AS ippt,
    (IFNULL(d.PIS, 0) + IFNULL(d.COFINS, 0)) AS pis_cofins,
    0 AS ipi,
    0 AS outros_impostos,
    IFNULL(d.COMISSAO, 0) AS comissao,
    d.CFOP AS cfop,
    d.UNIDADE_PRODUTO AS un
FROM ecf_venda_detalhe d
INNER JOIN ecf_venda_cabecalho c ON d.ID_ECF_VENDA_CABECALHO = c.ID AND d.ID_ECF_CAIXA = c.ID_ECF_CAIXA
INNER JOIN produto p ON d.ID_ECF_PRODUTO = p.ID
WHERE c.DATA_VENDA = %(data)s

UNION ALL

SELECT 
    'Geral' AS local_estoque,
    1 AS filial,
    CAST(COALESCE(ne.numero, ne.chave, ne.id) AS CHAR) AS documento,
    ne.id as id_documento,
    CAST(p.ID AS CHAR) AS codigo,
    COALESCE(nep.descricao_produto_ne, p.DESCRICAOPDV) AS nome,
    CAST(CONCAT(COALESCE(ne.data_chegada, ne.data_emissao), ' ', COALESCE(ne.hora_chegada, '00:00:00')) AS DATETIME) AS datahora,
    nep.id AS currenttimemillis,
    CAST(COALESCE(ne.modelo, '55') AS UNSIGNED) AS tipodocumento,
    nep.quantidade_produto_ne AS qtd,
    IF(ne.excluido = 1 OR ne.SITUACAO = 'C', 'C', 'E') AS tipo_movimentacao,
    nep.valor_total_produto_ne AS valortotal,
    IFNULL(p.VALOR_COMPRA, 0) AS precoultimacompra,
    IFNULL(nep.valor_unitario_custo_compra_produto_ne, nep.valor_unitario_produto_ne) AS custoaquisicao,
    0 AS customedio,
    IFNULL(nep.valor_icms_produto_ne, 0) AS icms,
    IFNULL(nep.valor_icms_st_produto_ne, 0) AS icms_st,
    IFNULL(p.IPPT, 0) AS ippt,
    (IFNULL(nep.v_pis, 0) + IFNULL(nep.v_cofins, 0)) AS pis_cofins,
    IFNULL(nep.v_ipi, 0) AS ipi,
    0 AS outros_impostos,
    0 AS comissao,
    nep.cfop_produto_ne AS cfop,
    nep.unidade_produto_ne AS un
FROM notas_entrada_produto nep
INNER JOIN notas_entrada ne ON nep.id_nota_entrada = ne.id
INNER JOIN produto p ON nep.id_produto = p.ID
WHERE COALESCE(ne.data_chegada, ne.data_emissao) = %(data)s;
