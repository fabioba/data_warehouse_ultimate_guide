/*
update the status for those customers that already exist in the db
 */
UPDATE sales.DIM_CUSTOMER
SET
    EFFECTIVE_END_DATE = CURRENT_DATE -1,
    IS_ACTIVE_FLAG = FALSE
WHERE
    CUSTOMER_ID IN (
        /*extract those customer_id where the loyalty_flag differ from the one stored in the db*/
        SELECT distinct
            s.CUSTOMER_ID
        FROM
            staging.STG_SALES s
            JOIN sales.DIM_CUSTOMER d ON s.CUSTOMER_ID = d.CUSTOMER_ID
        WHERE
            s.LOYALTY_CARD_FLAG <> d.LOYALTY_CARD_FLAG
            /*select active record for each customer_id*/
            AND d.is_active = TRUE
    );

INSERT INTO
    sales.DIM_CUSTOMER (
        CUSTOMER_ID,
        LOYALTY_CARD_FLAG,
        EFFECTIVE_START_DATE,
        EFFECTIVE_END_DATE,
        IS_ACTIVE_FLAG
    )
SELECT distinct
    s.CUSTOMER_ID,
    s.LOYALTY_CARD_FLAG,
    CURRENT_DATE as EFFECTIVE_START_DATE,
    '2999-12-31'::DATE as EFFECTIVE_END_DATE,
    TRUE as IS_ACTIVE_FLAG
FROM
    staging.STG_SALES s
    LEFT JOIN sales.DIM_CUSTOMER d ON s.CUSTOMER_ID = d.CUSTOMER_ID
    AND d.is_active_flag = TRUE
WHERE
    d.customer_id IS NULL
    OR s.LOYALTY_CARD_FLAG <> d.LOYALTY_CARD_FLAG;