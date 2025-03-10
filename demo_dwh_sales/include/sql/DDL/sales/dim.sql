
drop table sales.DIM_PRODUCT;
create table sales.DIM_PRODUCT(
	DIM_PRODUCT_ID INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,

    PRODUCT_ID INT NOT NULL,
    COST float,
    PRICE float,
    EFFECTIVE_START_DATE date,
    EFFECTIVE_END_DATE date,
    IS_ACTIVE_FLAG boolean default false,
	INSERT_TIMESTAMP timestamp default current_timestamp,
	LAST_UPDATE_TIMESTAMP timestamp default current_timestamp
);

drop table sales.DIM_CUSTOMER;
create table sales.DIM_CUSTOMER(
    DIM_CUSTOMER_ID INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    CUSTOMER_ID INT NOT NULL,
    LOYALTY_CARD_FLAG boolean,
    EFFECTIVE_START_DATE date,
    EFFECTIVE_END_DATE date,
    IS_ACTIVE_FLAG boolean default false,
	INSERT_TIMESTAMP timestamp default current_timestamp,
	LAST_UPDATE_TIMESTAMP timestamp default current_timestamp
);

drop table sales.DIM_PAYMENT;
create table sales.DIM_PAYMENT(
    DIM_PAYMENT_ID INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    PAYMENT_ID int NOT NULL,
    CREDIT_CARD varchar,
	INSERT_TIMESTAMP timestamp default current_timestamp
);

