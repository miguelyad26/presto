-- database: presto; groups: tpcds
SELECT sum(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM "tpcds"."sf1".web_sales,
     "tpcds"."sf1".item,
     "tpcds"."sf1".date_dim
WHERE i_manufact_id = 961
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN '2001-03-04' AND (cast('2001-03-04' AS date) + INTERVAL '90' DAY)
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt >
    (SELECT 1.3 * avg(ws_ext_discount_amt)
     FROM "tpcds"."sf1".web_sales,
          "tpcds"."sf1".date_dim
     WHERE ws_item_sk = i_item_sk
       AND d_date BETWEEN '2001-03-04' AND (cast('2001-03-04' AS date) + INTERVAL '90' DAY)
       AND d_date_sk = ws_sold_date_sk)
ORDER BY sum(ws_ext_discount_amt) LIMIT 100;
