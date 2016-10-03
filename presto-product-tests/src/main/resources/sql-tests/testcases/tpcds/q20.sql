-- database: presto; groups: tpcds
SELECT i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(cs_ext_sales_price) AS itemrevenue,
       sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over (partition BY i_class) AS revenueratio
FROM "tpcds"."sf1".catalog_sales,
     "tpcds"."sf1".item,
     "tpcds"."sf1".date_dim
WHERE cs_item_sk = i_item_sk
  AND i_category IN ('Women',
                     'Jewelry',
                     'Sports')
  AND cs_sold_date_sk = d_date_sk
  AND cast(d_date as date) BETWEEN cast('2002-06-20' AS date) AND (cast('2002-06-20' AS date) + INTERVAL '30' DAY)
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
