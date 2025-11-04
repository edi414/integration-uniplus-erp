SELECT DISTINCT cs."date"
FROM company_schedule cs
LEFT JOIN movimentacao_estoque me ON DATE(me.datahora) = cs."date"
WHERE me.datahora IS NULL
ORDER BY cs."date";

