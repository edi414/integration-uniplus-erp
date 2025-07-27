with nfes as (
    select distinct 
        runf.chave, 
        to_date(runf.data_emissao, 'DD/MM/YYYY') as data_emissao
    from report_uniplus_notas_fiscais runf
    left join (
        select distinct chave_nfe 
        from precificacao p
    ) as p on p.chave_nfe = runf.chave 
    where p.chave_nfe is null
        and runf.situacao = 'autorizada'
        and runf.status not in ('entrada_fornecedor', 'cancelada_pelo_fornecedor')
    order by 2 desc
)
select distinct chave 
from nfes 
where data_emissao >= current_date::date - 60 