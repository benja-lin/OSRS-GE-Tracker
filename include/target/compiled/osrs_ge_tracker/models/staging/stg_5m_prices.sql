

WITH deduped AS (
    SELECT
        data,
        "timestamp" AS price_timestamp,
        loaded_at
    FROM "osrs"."main"."raw_5m_prices"
    qualify row_number() OVER (PARTITION BY "timestamp" ORDER BY loaded_at asc) = 1
),
exploded AS (
    SELECT
        d.price_timestamp,
        d.loaded_at,
        item.key AS item_id,
        item.value ->> 'avgHighPrice' AS avg_high_price,
        item.value ->> 'highPriceVolume' AS high_price_volume,
        item.value ->> 'avgLowPrice' AS avg_low_price,
        item.value ->> 'lowPriceVolume' AS low_price_volume
    FROM deduped d, json_each(d.data) AS item
)

SELECT
    CAST(item_id AS bigint) AS item_id,
    price_timestamp,
    CAST(avg_high_price AS bigint) AS avg_high_price,
    CAST(high_price_volume AS bigint) AS high_price_volume,
    CAST(avg_low_price AS bigint) AS avg_low_price,
    CAST(low_price_volume AS bigint) AS low_price_volume,
    loaded_at
FROM exploded