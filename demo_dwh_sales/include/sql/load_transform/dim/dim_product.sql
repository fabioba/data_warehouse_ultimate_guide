/*
update the status for those products that already exist in the db
 */
UPDATE sales.DIM_PRODUCT
SET
    EFFECTIVE_END_DATE = CURRENT_DATE -1,
    IS_ACTIVE_FLAG = FALSE
WHERE
    PRODUCT_ID IN (
        /*extract those PRODUCT_ID where the loyalty_flag differ from the one stored in the db*/
        SELECT distinct
            s.PRODUCT_ID
        FROM
            staging.STG_SALES s
            JOIN sales.DIM_PRODUCT d ON s.PRODUCT_ID = d.PRODUCT_ID
        WHERE
            /*select those record of which either cost or price differ from the staging one*/
            (
            s.PRICE <> d.PRICE
            or s.COST <> d.COST
            )
            /*select active record for each PRODUCT_ID*/
            AND d.is_active = TRUE
    );

INSERT INTO
    sales.DIM_CUSTOMER (
        PRODUCT_ID,
        COST,
        PRICE,
        EFFECTIVE_START_DATE,
        EFFECTIVE_END_DATE,
        IS_ACTIVE_FLAG
    )
SELECT distinct
    s.PRODUCT_ID,
    s.COST,
    s.PRICE,
    CURRENT_DATE as EFFECTIVE_START_DATE,
    '2999-12-31'::DATE as EFFECTIVE_END_DATE,
    TRUE as IS_ACTIVE_FLAG
FROM
    staging.STG_SALES s
    LEFT JOIN sales.DIM_PRODUCT d ON s.PRODUCT_ID = d.PRODUCT_ID
    AND d.is_active_flag = TRUE
WHERE
    d.PRODUCT_ID IS NULL
    OR s.COST <> d.COST
    OR s.PRICE <> d.PRICE
    ;