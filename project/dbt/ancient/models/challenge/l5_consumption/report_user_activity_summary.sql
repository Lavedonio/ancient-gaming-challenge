SELECT
    id,
    name,
    registration_date,
    email,
    total_deposit,
    total_withdrawal,
    total_balance,
    preference_version,
    is_latest_preference,
    preferred_language,
    notifications_enabled,
    marketing_opt_in,
    user_preferences_created_at,
    user_preferences_updated_at
FROM
    {{ ref('user_activity_summary') }}
