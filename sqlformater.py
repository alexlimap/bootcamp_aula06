from sql_formatter.core import format_sql
path='''with datas as (SELECT table_name, date(last_modified) AS last_modified_date,last_modified FROM `api-conta-azul-424818.db_conta_azul.tb_atualzacoes`)
,todas as (select 'f_extrato' as table_name,id_cliente,max(date(date)) as data from `api-conta-azul-424818.db_conta_azul.f_extrato` group by 1,2
union all
select 'f_pagar' as table_name,id_cliente,max(date(dueDate)) as data from `api-conta-azul-424818.db_conta_azul.f_pagar` group by 1,2
union all
select 'f_receber' as table_name,id_cliente,max(date(dueDate)) as data from `api-conta-azul-424818.db_conta_azul.f_receber` group by 1,2)
select datas.table_name,
case when datas.table_name like 'd_%' then datas.last_modified_date
else '1999-01-01' end as datas
from datas
full join todas on datas.table_name = todas.table_name'''
print(format_sql(path))

