WITH source AS (
    SELECT
        comic_id,
        title
    FROM {{ ref('dim_comic') }}
),
metrics AS (
    SELECT
        comic_id,
        FLOOR(RANDOM() * 10000) AS views,
        LENGTH(title) * 5 AS cost,
        ROUND(CAST(RANDOM() * (10 - 1) + 1 AS NUMERIC), 1) AS customer_reviews
    FROM source
)

SELECT
    comic_id,
    views,
    cost,
    customer_reviews
FROM metrics
