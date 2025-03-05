INSERT INTO
    sales.DIM_PAYMENT (PAYMENT_ID int NOT NULL, CREDIT_CARD varchar,)
select distinct
    PAYMENT_ID,
    CREDIT_CARD
from
    sales.STG_SALES s
    left join sales.DIM_PAYMENT d on s.PAYMENT_ID = d.PAYMENT_ID
where
    d.PAYMENT_ID is NULL;