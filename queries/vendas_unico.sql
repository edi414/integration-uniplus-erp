SELECT
    m2.id                                                      AS id_documento,
    m2.filial::text                                            AS filial,
    m2.pdv::int                                                AS pdv,
    m2.data                                                    AS emissao,
    m2.horainicial                                             AS hora,
    CASE
        WHEN m2.serienfce IS NOT NULL AND m2.numeronfce IS NOT NULL
            THEN m2.serienfce || '/' || m2.numeronfce
        ELSE NULL
    END                                                        AS documento,
    NULL                                                       AS ccf,
    m2.valorbruto                                              AS v_bruto,
    m2.descontoitem                                            AS desconto,
    m2.acrescimoitem                                           AS acrescimo,
    m2.valorliquido                                            AS v_liquido,
    CASE WHEN m2.cancelado = 1 THEN TRUE ELSE FALSE END        AS canc,
    NULL                                                       AS cliente,
    NULL                                                       AS cnpj_cpf,
    f.abreviacao                                               AS finalizador,
    p.valortotal                                               AS valor_finalizador,
    m2.horafinal                                               AS hora_final,
    p.troco                                                    AS troco,
    NULL                                                       AS status_nfce,
    p.valortotal                                               AS valor_recebido

FROM operacao m2

-- Finalizador predominante (maior valor) — replicando lógica do ETL G3
LEFT JOIN (
    SELECT
        idoperacao,
        finalizador,
        valortotal,
        troco,
        ROW_NUMBER() OVER (PARTITION BY idoperacao ORDER BY valortotal DESC, id DESC) AS rn
    FROM pagamento
) p ON p.idoperacao = m2.id AND p.rn = 1

LEFT JOIN finalizador f ON f.id = p.finalizador

WHERE m2.data = %(data)s
  AND m2.tipo = 1;
