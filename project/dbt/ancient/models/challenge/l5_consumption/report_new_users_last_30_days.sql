SELECT
    id,
    name,
    registration_date,
    email
FROM
    {{ ref('new_users_last_30days') }}
