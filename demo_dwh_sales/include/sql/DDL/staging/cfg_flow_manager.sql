create table staging.CFG_FLOW_MANAGER(
    PROCESS varchar,
    LAST_RUN timestamp,
    INSERT_TIMESTAMP timestamp default current_timestamp,
    primary key(PROCESS)
);