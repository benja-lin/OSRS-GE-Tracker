SELECT
    id AS item_id,
    name AS item_name,
    examine AS item_description,
    members,
    lowalch,
    highalch,
    "value" AS item_value,
    "limit" AS buy_limit,
    loaded_at
FROM "osrs"."main"."raw_mapping"