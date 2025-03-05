with STG_SALES as(
    select
        coalesce(transaction_id, -1) as transaction_id,
        coalesce(transactional_date, '1900-01-01') as transactional_date,
        coalesce(product_id, -1) as product_id,
        coalesce(customer_id, -1) as customer_id,
        coalesce(payment, 'ND') as payment,
        coalesce(credit_card, -1) as credit_card,
        case when loyalty_card = 'F' then FALSE 
        when loyalty_card = 'T' then TRUE
        else FALSE end as loyalty_card_flag,
        coalesce(cost, 0) as cost,
        coalesce(quantity, 0) as quantity,
        coalesce(price, 0) as price
    from
        staging.SALES
    where
        INSERT_TIMESTAMP > (
            select
                LAST_VALUE
            from
                sales.CFG_FLOW_MANAGER
            where
                PROCESS = 'LOAD_TRANSFORM'
        )

)

/*
perform some basic transformation before
*/
insert into sales.STG_SALES

    select distinct
        coalesce(transaction_id, -1) as transaction_id,
        coalesce(transactional_date, '1900-01-01') as transactional_date,
        coalesce(product_id, -1) as product_id,
        coalesce(customer_id, -1) as customer_id,
        coalesce(payment, 'ND') as payment,
        coalesce(credit_card, -1) as credit_card,
        case when loyalty_card = 'F' then FALSE 
        when loyalty_card = 'T' then TRUE
        else FALSE end as loyalty_card_flag,
        coalesce(cost, 0) as cost,
        coalesce(quantity, 0) as quantity,
        coalesce(price, 0) as price
    from STG_SALES
    ;