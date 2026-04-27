SELECT DISTINCT cs."date"
FROM company_schedule cs
LEFT JOIN uniplus_vendas_pdvs v ON v.emissao = cs."date"
WHERE v.emissao IS NULL
  AND cs."date" >= '2026-04-01'
ORDER BY cs."date";