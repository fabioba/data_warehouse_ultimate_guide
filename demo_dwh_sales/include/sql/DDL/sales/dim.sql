create table sales.DIM_PRODUCT(
    DIM_PRODUCT_ID INT IDENTITY(1,1),
    PRODUCT_ID INT NOT NULL,
    COST float,
    PRICE float,
    EFFECTIVE_START_DATE datetime,
    EFFECTIVE_END_DATE datetime,
    IS_ACTIVE_FLAG boolean default false,
    PRIMARY KEY(DIM_PRODUCT_ID)
);

create table sales.DIM_CUSTOMER(
    DIM_CUSTOMER_ID INT IDENTITY(1,1),
    CUSTOMER_ID INT NOT NULL,
    LOYALTY_CARD_FLAG boolean,
    EFFECTIVE_START_DATE datetime,
    EFFECTIVE_END_DATE datetime,
    IS_ACTIVE_FLAG boolean default false,
    PRIMARY KEY(DIM_CUSTOMER_ID)
);


create table sales.DIM_PAYMENT(
    DIM_PAYMENT_ID INT IDENTITY(1,1),
    PAYMENT_ID int NOT NULL,
    CREDIT_CARD varchar,
    PRIMARY KEY(DIM_PAYMENT_ID)
);

