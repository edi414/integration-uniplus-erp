SELECT 
    data_emissao, 
    cod_filial, 
    aliquota_icms, 
    base_calculo, 
    valor_icms, 
    tipo_operacao 
FROM fiscal.notas_fiscais nf 
WHERE data_emissao = %(data)s 