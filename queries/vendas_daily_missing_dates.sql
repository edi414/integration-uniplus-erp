SELECT DISTINCT cs."date"
FROM company_schedule cs
LEFT JOIN uniplus_vendas_pdvs v ON v.emissao = cs."date"
WHERE cs."date" >= '2026-04-01'
  AND (
    v.emissao IS NULL
    OR cs."date" >= CURRENT_DATE - INTERVAL '7 days'
  )
ORDER BY cs."date";