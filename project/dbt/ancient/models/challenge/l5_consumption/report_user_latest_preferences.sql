SELECT
    id,
    name,
    registration_date,
    email,
    preferred_language,
    notifications_enabled,
    marketing_opt_in,
    user_preferences_created_at,
    user_preferences_updated_at
FROM
    {{ ref('user_latest_preferences') }}
