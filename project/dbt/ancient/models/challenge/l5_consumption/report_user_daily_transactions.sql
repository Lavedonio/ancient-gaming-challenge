SELECT
    user_id,
    transaction_date,
    total_deposit,
    total_withdrawal
FROM
    {{ ref('user_daily_transactions') }}
