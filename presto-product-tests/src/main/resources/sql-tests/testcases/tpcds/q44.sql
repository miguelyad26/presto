-- database: presto; groups: tpcds
SELECT asceding.rnk,
       i1.i_product_name best_performing,
       i2.i_product_name worst_performing
FROM
  (SELECT *
   FROM
     (SELECT item_sk,
             rank() over (
                          ORDER BY rank_col ASC) rnk
      FROM
        (SELECT ss_item_sk item_sk,
                avg(ss_net_profit) rank_col
         FROM "tpcds"."sf1".store_sales ss1
         WHERE ss_store_sk = 21
         GROUP BY ss_item_sk
         HAVING avg(ss_net_profit) > 0.9*
           (SELECT avg(ss_net_profit) rank_col
            FROM "tpcds"."sf1".store_sales
            WHERE ss_store_sk = 21
              AND ss_hdemo_sk IS NULL
            GROUP BY ss_store_sk))v1)v11
   WHERE rnk < 11) asceding,

  (SELECT *
   FROM
     (SELECT item_sk,
             rank() over (
                          ORDER BY rank_col DESC) rnk
      FROM
        (SELECT ss_item_sk item_sk,
                avg(ss_net_profit) rank_col
         FROM "tpcds"."sf1".store_sales ss1
         WHERE ss_store_sk = 21
         GROUP BY ss_item_sk
         HAVING avg(ss_net_profit) > 0.9*
           (SELECT avg(ss_net_profit) rank_col
            FROM "tpcds"."sf1".store_sales
            WHERE ss_store_sk = 21
              AND ss_hdemo_sk IS NULL
            GROUP BY ss_store_sk))v2)v21
   WHERE rnk < 11) descending,
     "tpcds"."sf1".item i1,
     "tpcds"."sf1".item i2
WHERE asceding.rnk = descending.rnk
  AND i1.i_item_sk=asceding.item_sk
  AND i2.i_item_sk=descending.item_sk
ORDER BY asceding.rnk LIMIT 100;
