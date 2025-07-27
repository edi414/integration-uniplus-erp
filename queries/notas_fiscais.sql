SELECT  
    df.numerodocumento AS id_uniplus,
    df.chaveacesso AS chave,
    df.emissao AS data_emissao,  
    df.razaosocial AS fornecedor,  
    df.cnpjcpf AS cpnj_cpf,  
    df.valor AS valor,
    n.datainclusao AS data_inclusao,
    df.dataprimeirovencimento AS vencimento,

    CASE 
        WHEN df.situacaonfe = 1 THEN 'autorizada'
        WHEN df.situacaonfe = 3 THEN 'cancelada'
        ELSE 'nao_autorizada'
    END AS status_nfe,

    df.situacaomanifestacao,
    CASE 
        WHEN df.situacaodocumentofiscal = 1 THEN 'pendente_processamento'
        WHEN df.situacaodocumentofiscal = 2 THEN 'importacao_pendente'
        WHEN df.situacaodocumentofiscal = 3 THEN 'liberacao_pendente'
        WHEN df.situacaodocumentofiscal = 4 THEN 'xml_importado'
        WHEN df.situacaodocumentofiscal = 5 THEN 'exclusao_pendente'
        WHEN df.situacaodocumentofiscal = 6 THEN 'cancelada_pelo_fornecedor'
        WHEN df.situacaodocumentofiscal = 8 THEN 'entrada_fornecedor'
        ELSE 'desconhecida'
    END AS status_documento_fiscal,
    false as processed,
    df.arquivoxml

FROM  
    documentofiscalfornecedor df 
LEFT JOIN notafiscal n
    ON df.chaveacesso = n.chavenfe AND n.tipodocumento = 'E'
LEFT JOIN cfop c  
    ON c.id = n.idcfop  
WHERE 1=1
ORDER BY  
    df.emissao DESC 