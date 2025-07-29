WITH nfes AS (
    SELECT DISTINCT 
        runf.chave, 
        runf.data_emissao
    FROM report_uniplus_notas_fiscais runf
    LEFT JOIN (
        SELECT DISTINCT chave_nfe 
        FROM precificacao p
    ) AS p ON p.chave_nfe = runf.chave 
    WHERE p.chave_nfe IS NULL
      AND runf.situacao = 'autorizada'
      AND runf.status NOT IN ('entrada_fornecedor', 'cancelada_pelo_fornecedor')
      and data_emissao >= current_date - INTERVAL '60 days'
    ORDER BY runf.data_emissao desc
)
SELECT DISTINCT chave 
FROM nfes 
