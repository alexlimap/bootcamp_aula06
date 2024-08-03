with datas as (
    SELECT
        table_name,
        date(last_modified) as last_modified_date,
        last_modified
    FROM
        `api-conta-azul-424818.db_conta_azul.tb_atualzacoes`
),
todas as (
    SELECT
        'f_extrato' as table_name,
        id_cliente,
        max(date(date)) as data
    FROM
        `api-conta-azul-424818.db_conta_azul.f_extrato`
    GROUP BY
        1,
        2
    UNION
    all
    SELECT
        'f_pagar' as table_name,
        id_cliente,
        max(date(duedate)) as data
    FROM
        `api-conta-azul-424818.db_conta_azul.f_pagar`
    GROUP BY
        1,
        2
    UNION
    all
    SELECT
        'f_receber' as table_name,
        id_cliente,
        max(date(duedate)) as data
    FROM
        `api-conta-azul-424818.db_conta_azul.f_receber`
    GROUP BY
        1,
        2
)
SELECT
    datas.table_name,
    case
        when datas.table_name like 'd_%' then datas.last_modified_date
        else '1999-01-01'
    end as datas
FROM
    datas full
    join todas ON datas.table_name = todas.table_name