-- database: presto; groups: tpcds
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(ws_ext_sales_price) AS itemrevenue,
       sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over (partition BY i_class) AS revenueratio
FROM "tpcds"."sf1".web_sales,
     "tpcds"."sf1".item,
     "tpcds"."sf1".date_dim
WHERE ws_item_sk = i_item_sk
  AND i_category IN ('Sports',
                     'Books',
                     'Home')
  AND ws_sold_date_sk = d_date_sk
  AND cast(d_date as date) BETWEEN cast('1999-02-22' AS date) AND (cast('1999-02-22' AS date) + INTERVAL '30' DAY)
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio LIMIT 100;