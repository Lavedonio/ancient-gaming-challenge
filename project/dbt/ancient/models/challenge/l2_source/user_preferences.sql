/*
The user preference's table
*/

SELECT
    id,
    user_id,
    preferred_language,
    notifications_enabled,
    marketing_opt_in,
    event_timestamp AS created_at,
    IFNULL(LEAD(event_timestamp) OVER preferences_window, event_timestamp) AS updated_at
FROM
    {{ source('l1_landing', 'raw_user_preferences') }}
WINDOW
    preferences_window AS (PARTITION BY user_id ORDER BY event_timestamp)
