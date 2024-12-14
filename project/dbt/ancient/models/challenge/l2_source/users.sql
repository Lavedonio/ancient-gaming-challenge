/*
The user's table
*/

SELECT * FROM {{ source('l1_landing', 'raw_users') }}
