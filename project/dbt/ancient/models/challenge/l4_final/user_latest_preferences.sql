/*
This report solves the following business question:

"Write a query that shows all users along with their latest preferences."
*/

SELECT
    users.id,
    users.name,
    users.registration_date,
    users.email,
    user_preferences_extra_info.preferred_language,
    user_preferences_extra_info.notifications_enabled,
    user_preferences_extra_info.marketing_opt_in,
    user_preferences_extra_info.created_at AS user_preferences_created_at,
    user_preferences_extra_info.updated_at AS user_preferences_updated_at
FROM
    {{ ref('user_preferences_extra_info') }} user_preferences_extra_info
    JOIN {{ ref('users') }} users
        ON users.id = user_preferences_extra_info.user_id
WHERE
    user_preferences_extra_info.is_latest_preference
