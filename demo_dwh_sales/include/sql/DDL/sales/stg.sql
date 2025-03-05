
create table sales.STG_SALES(
    transaction_id int,
    transactional_date timestamp,
    product_id int,
    customer_id int,
    payment varchar,
    credit_card bigint,
    loyalty_card varchar,
    cost float,
    quantity float,
    price float,
    insert_timestamp timestamp default current_timestamp,
    last_update_timestamp timestamp default current_timestamp
);
