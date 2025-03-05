insert into
    staging.SALES
select
    transaction_id,
    transactional_date,
    product_id,
    customer_id,
    payment,
    credit_card,
    loyalty_card,
    cost,
    quantity,
    price
from
    raw.SALES
where
    INSERT_TIMESTAMP > (
        select
            LAST_VALUE
        from
            staging.CFG_FLOW_MANAGER
        where
            PROCESS = 'EXTRACT'
    );