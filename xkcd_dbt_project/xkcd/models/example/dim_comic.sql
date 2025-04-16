WITH source AS (
    SELECT
        comic_id,
        title,
        safe_title,
        transcript,
        alt,
        image_url,
        date,
        news
    FROM {{ source('xkcd', 'comics') }}
)

SELECT
    comic_id,
    title,
    safe_title,
    transcript,
    alt,
    image_url,
    date,
    news
FROM source
