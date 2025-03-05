insert into sales.STG_SALES

    select *
    from raw.SALES
    where INSERT_TIMESTAMP > (
        select LAST_VALUE
        from sales.CFG_FLOW_MANAGER
        where PROCESS = 'EXTRACT'
        );