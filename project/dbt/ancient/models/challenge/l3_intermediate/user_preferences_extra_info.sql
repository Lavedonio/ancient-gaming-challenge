/*
This table adds relevant info to user_preference table
*/

WITH latest_preference AS (
    SELECT
        id,
        MAX(updated_at) AS latest_date
    FROM
        {{ ref('user_preferences') }}
    GROUP BY
        id
)

SELECT
    user_preferences.id,
    user_preferences.user_id,
    RANK() OVER preferences_window AS preference_version,
    latest_preference.latest_date IS NOT NULL AS is_latest_preference,
    user_preferences.preferred_language,
    user_preferences.notifications_enabled,
    user_preferences.marketing_opt_in,
    user_preferences.created_at,
    user_preferences.updated_at
FROM
    {{ ref('user_preferences') }} user_preferences
    LEFT JOIN latest_preference
        ON user_preferences.id = latest_preference.id
            AND user_preferences.updated_at = latest_preference.latest_date
WINDOW
    preferences_window AS (PARTITION BY user_id ORDER BY created_at)
