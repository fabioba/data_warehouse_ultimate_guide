
insert into sales.FCT_TRANSACTIONS
    (
        TRANSACTION_ID,
        TRANSACTION_DATE,
        DIM_PRODUCT_ID,
        DIM_CUSTOMER_ID,
        DIM_PAYMENT_ID,
        TOTAL_PRICE,
        TOTAL_COST,
        PROFIT
    )
    select distinct
        TRANSACTION_ID,
        TRANSACTION_DATE,
        DIM_PRODUCT_ID,
        DIM_CUSTOMER_ID,
        DIM_PAYMENT_ID,
        TOTAL_PRICE,
        TOTAL_COST,
        PROFIT
    from sales.FCT_TRANSACTIONS_CURRENT_RUN;
