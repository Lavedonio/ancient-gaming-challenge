/*
This report solves the following business question:

"Write a query that sums transaction amounts by date and user, with separate columns for deposits and withdrawals (withdrawals should be negative)."
*/

SELECT
    user_id,
    transaction_date,
    SUM(IF(type = 'deposit', total_daily_amount, 0)) AS total_deposit,
    SUM(IF(type = 'withdrawal', total_daily_amount, 0)) AS total_withdrawal
FROM
    {{ ref('helper_user_daily_transactions') }}
GROUP BY
    user_id,
    transaction_date
