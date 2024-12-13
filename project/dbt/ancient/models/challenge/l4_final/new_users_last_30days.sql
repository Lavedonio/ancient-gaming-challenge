/*
This report solves the following business question:

"Write a query that returns all users who made a deposit in the last 30 days"
*/

WITH valid_user_ids AS (
    SELECT DISTINCT user_id
    FROM {{ ref('transactions') }}
    WHERE
        type = 'deposit'
        AND transaction_date > DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
)


SELECT
    users.id,
    users.name,
    users.registration_date,
    users.email
FROM
    {{ ref('users') }} users
    JOIN valid_user_ids ON users.id = valid_user_ids.user_id
