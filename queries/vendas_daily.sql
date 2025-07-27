select 
    m2.id, 
    m2.data, 
    m2.filial, 
    m2.pdv, 
    u.nome as usuario, 
    m2.valorbruto, 
    m2.valorliquido, 
    m2.cancelado, 
    m2.serienfce, 
    m2.numeronfce, 
    p2.abreviacao as finalizador, 
    p.troco, 
    p.valortotal, 
    m2.descontoitem, 
    m2.acrescimoitem, 
    m2.horainicial, 
    m2.horafinal 
from operacao m2 
left join usuario u on u.id::text = m2.usuario 
left join pagamento p on p.idoperacao = m2.id 
left join finalizador p2 on p.finalizador = p2.id 
where m2.data = %(data)s 
    and m2.tipo = 1 