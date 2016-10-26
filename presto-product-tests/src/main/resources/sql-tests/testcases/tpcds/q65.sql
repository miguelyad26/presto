-- database: presto; groups: tpcds
SELECT s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
FROM "tpcds"."sf1".store,
     "tpcds"."sf1".item,

  (SELECT ss_store_sk,
          avg(revenue) AS ave
   FROM
     (SELECT ss_store_sk,
             ss_item_sk,
             sum(ss_sales_price) AS revenue
      FROM "tpcds"."sf1".store_sales,
           "tpcds"."sf1".date_dim
      WHERE ss_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1176 AND 1176+11
      GROUP BY ss_store_sk,
               ss_item_sk) sa
   GROUP BY ss_store_sk) sb,

  (SELECT ss_store_sk,
          ss_item_sk,
          sum(ss_sales_price) AS revenue
   FROM "tpcds"."sf1".store_sales,
        "tpcds"."sf1".date_dim
   WHERE ss_sold_date_sk = d_date_sk
     AND d_month_seq BETWEEN 1176 AND 1176+11
   GROUP BY ss_store_sk,
            ss_item_sk) sc
WHERE sb.ss_store_sk = sc.ss_store_sk
  AND sc.revenue <= 0.1 * sb.ave
  AND s_store_sk = sc.ss_store_sk
  AND i_item_sk = sc.ss_item_sk
ORDER BY s_store_name,
         i_item_desc LIMIT 100;