-- database: presto; groups: tpcds
SELECT sum(cs_ext_discount_amt) AS "excess discount amount"
FROM "tpcds"."sf1".catalog_sales,
     "tpcds"."sf1".item,
     "tpcds"."sf1".date_dim
WHERE i_manufact_id = 284
  AND i_item_sk = cs_item_sk
  AND d_date BETWEEN '2001-01-07' AND (cast('2001-01-07' AS date) + INTERVAL '90' DAY)
  AND d_date_sk = cs_sold_date_sk
  AND cs_ext_discount_amt >
    (SELECT 1.3 * avg(cs_ext_discount_amt)
     FROM "tpcds"."sf1".catalog_sales,
          "tpcds"."sf1".date_dim
     WHERE cs_item_sk = i_item_sk
       AND d_date BETWEEN '2001-01-07' AND (cast('2001-01-07' AS date) + INTERVAL '90' DAY)
       AND d_date_sk = cs_sold_date_sk) LIMIT 100;
