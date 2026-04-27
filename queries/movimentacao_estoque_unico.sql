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
        WHEN m.tipodocumento::text IN ('2', '3') THEN n.numeronotafiscal::text
        WHEN m.tipodocumento::text IN ('1') THEN o.serienfce::text || '/' || o.numeronfce::text
        ELSE NULL
    END AS documento,
    m.idoriginal AS id_documento,
    p.codigo,
    p.nome,
    m.datahora,
    m.currenttimemillis,
    m.tipodocumento,
    CASE
        WHEN m.quantidadeentrada IS NULL OR m.quantidadeentrada = 0 THEN m.quantidadesaida
        ELSE m.quantidadeentrada
    END AS qtd,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN 'E'
        WHEN m.tipodocumento::text IN ('1') THEN 'S'
        ELSE NULL
    END AS tipo_movimentacao,
    m.valortotal,
    m.precoultimacompra,
    m.custoaquisicao,
    m.customedio,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN n2.icms
        WHEN m.tipodocumento::text IN ('1') THEN i.icms
        ELSE NULL
    END AS icms,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN n2.icmssubstituicao
        WHEN m.tipodocumento::text IN ('1') THEN NULL
        ELSE NULL
    END AS icms_st,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN n2.tributacao
        WHEN m.tipodocumento::text IN ('1') THEN i.ippt
        ELSE NULL
    END AS ippt,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN (COALESCE(n2.pis, 0) + COALESCE(n2.cofins, 0))
        WHEN m.tipodocumento::text IN ('1') THEN (COALESCE(i.pis, 0) + COALESCE(i.cofins, 0))
        ELSE NULL
    END AS pis_cofins,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN n2.ipi
        WHEN m.tipodocumento::text IN ('1') THEN NULL
        ELSE NULL
    END AS ipi,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN n2.outrosimpostospreco
        WHEN m.tipodocumento::text IN ('1') THEN NULL
        ELSE NULL
    END AS outros_impostos,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN n2.comissao
        WHEN m.tipodocumento::text IN ('1') THEN i.comissao
        ELSE NULL
    END AS comissao,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN n2.cfop
        WHEN m.tipodocumento::text IN ('1') THEN i.cfop
        ELSE NULL
    END AS cfop,
    CASE
        WHEN m.tipodocumento::text IN ('2', '3') THEN n2.unidade::text
        WHEN m.tipodocumento::text IN ('1') THEN i.unidademedida::text
        ELSE NULL
    END AS un

FROM (
    -- Dedup: mesma chave natural pode ter duplicatas no Unico; mantém o id maior
    SELECT DISTINCT ON (currenttimemillis, idproduto, idoriginal, tipodocumento)
        *
    FROM public.movimentoestoque
    WHERE DATE(datahora) = %(data)s
      AND cancelado = 0
    ORDER BY currenttimemillis, idproduto, idoriginal, tipodocumento, id DESC
) m
INNER JOIN public.produto p
    ON p.id::text = m.idproduto::text
LEFT JOIN public.operacao o
    ON m.idoriginal = o.id
LEFT JOIN public.notafiscal n
    ON m.idoriginal = n.id
LEFT JOIN ult_notafiscalitem n2
    ON m.idoriginal = n2.idnotafiscal AND p.codigo = n2.produto
LEFT JOIN ult_item i
    ON o.id = i.idoperacao AND p.codigo = i.produto;
