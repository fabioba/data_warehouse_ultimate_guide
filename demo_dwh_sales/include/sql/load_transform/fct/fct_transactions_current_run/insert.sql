insert into sales.FCT_TRANSACTIONS_CURRENT_RUN

    select distinct
        s.TRANSACTION_ID,
        s.TRANSACTION_DATE,
        dp.DIM_PRODUCT_ID,
        dc.DIM_CUSTOMER_ID,
        dpy.DIM_PAYMENT_ID,
        dc.PRICE * s.QUANTITY as TOTAL_PRICE,
        dc.COST * s.QUANTITY as TOTAL_COST,
        coalesce(dc.PRICE * s.QUANTITY - dc.COST * s.QUANTITY, 0) as PROFIT
    from sales.STG_SALES s
    left join sales.DIM_PRODUCT dp
        on s.PRODUCT_ID = dp.PRODUCT_ID
        and s.TRANSACTION_DATE between dp.EFFECTIVE_START_DATE and dp.EFFECTIVE_END_DATE

    left join sales.DIM_CUSTOMER dc
        on s.CUSTOMER_ID = dc.CUSTOMER_ID
        and s.TRANSACTION_DATE between dc.EFFECTIVE_START_DATE and dc.EFFECTIVE_END_DATE


    left join sales.DIM_PAYMENT dpy
        on s.PAYMENT_ID = dpy.PAYMENT_ID
        and s.TRANSACTION_DATE between dc.EFFECTIVE_START_DATE and dc.EFFECTIVE_END_DATE

;
