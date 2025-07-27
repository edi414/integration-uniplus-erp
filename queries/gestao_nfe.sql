SELECT 
    df.numerodocumento, 
    n.numeronotafiscal, 
    df.chaveacesso, 
    n.status, 
    df.razaosocial, 
    df.cnpjcpf, 
    df.valor, 
    n.descontosubtotal, 
    n.avista, 
    n.aprazo, 
    df.emissao, 
    n.datainclusao, 
    n.vencimentocheque, 
    df.dataprimeirovencimento, 
    df.situacaonfe, 
    df.situacaomanifestacao, 
    df.situacaodocumentofiscal, 
    df.arquivoxml, 
    c.codigo AS cfop, 
    n.infocomppersonalizada 
FROM documentofiscalfornecedor df 
LEFT JOIN notafiscal n ON df.chaveacesso = n.chavenfe and n.tipodocumento = 'E' 
LEFT JOIN cfop c ON c.id = n.idcfop 
ORDER BY n.emissao DESC 