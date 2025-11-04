WITH ult_notafiscalitem AS (
    SELECT DISTINCT ON (idnotafiscal, produto)
        *
    FROM public.notafiscalitem
    ORDER BY idnotafiscal, produto, id DESC
),
ult_item AS (
    SELECT DISTINCT ON (idoperacao, produto)
        *
    FROM public.item
    ORDER BY idoperacao, produto, id DESC
)
SELECT
    'Geral' AS local_estoque,
    1 AS filial,
    CASE
        WHEN m.tipodocumento IN ('2', '3') THEN n.numeronotafiscal::text
        WHEN m.tipodocumento IN (1) THEN o.serienfce::text || '/' || o.numeronfce::text
        ELSE NULL
    END AS documento,
    p.codigo,
    m.datahora,
    m.currenttimemillis,
    m.tipodocumento,
    CASE
        WHEN m.quantidadeentrada IS NULL OR m.quantidadeentrada = 0 THEN m.quantidadesaida
        ELSE m.quantidadeentrada
    END AS qtd,
    m.valortotal,
    CASE
        WHEN m.tipodocumento IN ('2', '3') THEN n2.unidade::text
        WHEN m.tipodocumento IN (1) THEN i.unidademedida::text
        ELSE NULL
    END AS un,
    CASE 
        WHEN m.tipodocumento IN (2, 3) THEN 'E'
        WHEN m.tipodocumento IN (1) THEN 'S'
        ELSE NULL
    END AS tipo_movimentacao,
    p.nome
FROM public.movimentoestoque m
INNER JOIN public.produto p
    ON p.id::text = m.idproduto::text
LEFT JOIN public.operacao o
    ON m.idoriginal = o.id
LEFT JOIN public.notafiscal n
    ON m.idoriginal = n.id
LEFT JOIN ult_notafiscalitem n2 
    ON m.idoriginal = n2.idnotafiscal AND p.codigo = n2.produto 
LEFT JOIN ult_item i 
    ON o.id = i.idoperacao AND p.codigo = i.produto
WHERE DATE(m.datahora) = %(data)s
  AND m.cancelado = 0;

