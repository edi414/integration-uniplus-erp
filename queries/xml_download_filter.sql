SELECT chave 
FROM notas_fiscais 
WHERE arquivo_xml IS NULL 
  AND status_xml = 'XML Disponível'
LIMIT 30;
