SELECT
  f.idorigem           AS id_origem,
  f.tipo               AS tipo,
  split_part(f.documento, '/', 1) AS documento,
  d.razaosocial        AS razao_social,
  f.status             AS status,
  
  f.parcela            AS parcela,
  f.valor              AS valor,
  f.saldo              AS saldo,

  f.emissao            AS emissao,
  f.vencimentooriginal AS vencimento_original,
  f.vencimento         AS vencimento,
  f.entrada            AS entrada,
  f.pagamento          AS pagamento,
  f.baixa              AS baixa,
  f.registro           AS registro,

  f.historico          AS historico,
  f.codigobarras       AS codigo_barras,
  f.codigodigitado     AS codigo_digitado

FROM financeiro f
LEFT JOIN documentofiscalfornecedor d
  ON split_part(f.documento, '/', 1) = d.numerodocumento
  AND f.idorigem = d.idnotafiscal
WHERE f.tipo = 'P';


