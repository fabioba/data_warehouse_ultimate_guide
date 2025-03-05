/*
remove from the current run table, those records that are already ingested before and without any changes in the
dimensional values (product id, customer id, ...)
*/
delete from sales.FCT_TRANSACTIONS_CURRENT_RUN
where TRANSACTION_ID in (
    select  distinct ftcr.TRANSACTION_ID
    from sales.FCT_TRANSACTIONS_CURRENT_RUN ftcr
    left join on sales.FCT_TRANSACTIONS ft
        on ftcr.TRANSACTION_ID = ft.TRANSACTION_ID
        and ftcr.TRANSACTION_DATE = ft.TRANSACTION_DATE
        and ftcr.DIM_PRODUCT_ID = ft.DIM_PRODUCT_ID
        and ftcr.DIM_CUSTOMER_ID = ft.DIM_CUSTOMER_ID
        and ftcr.DIM_PAYMENT_ID = ft.DIM_PAYMENT_ID
);

/*
for those records from the current run, set the deleted_flag = true because some attribtued changed
*/
update sales.FCT_TRANSACTIONS
set DELETED_FLAG = TRUE
where TRANSACTION_ID in (
    select TRANSACTION_ID
    from sales.FCT_TRANSACTIONS_CURRENT_RUN
)

