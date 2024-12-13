-- Create a new table that joins Users, Transactions, and UserPreferences on user_id, and write a script to insert data into this combined table.


WITH transactions_summary AS (
    SELECT
        user_id,
        SUM(IF(type = 'deposit', total_daily_amount, 0)) AS total_deposit,
        SUM(IF(type = 'withdrawal', total_daily_amount, 0)) AS total_withdrawal
    FROM
        {{ ref('helper_user_daily_transactions') }}
    GROUP BY
        user_id
)

SELECT
    users.id,
    users.name,
    users.registration_date,
    users.email,
    transactions_summary.total_deposit,
    transactions_summary.total_withdrawal,
    transactions_summary.total_deposit - transactions_summary.total_withdrawal AS total_balance,
    user_preferences_extra_info.preference_version,
    user_preferences_extra_info.is_latest_preference,
    user_preferences_extra_info.preferred_language,
    user_preferences_extra_info.notifications_enabled,
    user_preferences_extra_info.marketing_opt_in,
    user_preferences_extra_info.created_at AS user_preferences_created_at,
    user_preferences_extra_info.updated_at AS user_preferences_updated_at
FROM
    {{ ref('user_preferences_extra_info') }} user_preferences_extra_info
    JOIN {{ ref('users') }} users
        ON users.id = user_preferences_extra_info.user_id
    JOIN transactions_summary
        ON users.id = transactions_summary.user_id
