create table sales.FCT_TRANSACTIONS(
    FCT_TRANSACTION_ID INT IDENTITY(1,1),
    TRANSACTION_ID INT NOT NULL,
    DIM_CUSTOMER_ID  INT NOT NULL,
    DIM_PRODUCT_ID  INT NOT NULL,
    DIM_PAYMENT_ID  INT NOT NULL,
    TOTAL_PRICE FLOAT,
    TOTAL_COST FLOAT,
    PROFIT FLOAT,
    INSERT_TIMESTAMP timestamp default current_timestamp,
    PRIMARY KEY(FCT_TRANSACTION_ID)
);