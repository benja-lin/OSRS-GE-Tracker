
    

    create  table
      "osrs"."main"."fct_item_prices__dbt_tmp"
  
    
    as (
      

SELECT  m.item_id, m.item_name, m.item_description, m.members, m.lowalch, m.highalch, m.item_value, m.buy_limit,
    m.loaded_at AS mapping_loaded_at,
    p.price_timestamp,
    p.avg_high_price,
    p.high_price_volume,
    p.avg_low_price,
    p.low_price_volume,
    p.loaded_at AS price_loaded_at
FROM "osrs"."main"."stg_5m_prices" AS p
LEFT JOIN "osrs"."main"."stg_mapping" AS m ON m.item_id = p.item_id
    );
    
  