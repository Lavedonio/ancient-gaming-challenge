/*
This model combines the multiple transactions that happen in the same day into 1 row, by transaction type and user.
*/

SELECT
    user_id,
    transaction_date,
    type,
    SUM(ROUND(CAST(amount AS NUMERIC), 2)) AS total_daily_amount
FROM
    {{ ref('transactions') }}
GROUP BY
    user_id,
    transaction_date,
    type
