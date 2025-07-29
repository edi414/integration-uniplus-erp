SELECT DISTINCT cs."date"
FROM company_schedule cs
LEFT JOIN uniplus_vendas_pdvs v ON v.emissao = cs."date"
WHERE v.emissao IS NULL
ORDER BY cs."date"; 