-- database: presto; groups: tpcds
SELECT *
FROM
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sumsales,
          rank() over (partition BY i_category
                       ORDER BY sumsales DESC) rk
   FROM
     (SELECT i_category,
             i_class,
             i_brand,
             i_product_name,
             d_year,
             d_qoy,
             d_moy,
             s_store_id,
             sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
      FROM "tpcds"."sf1".store_sales,
           "tpcds"."sf1".date_dim,
           "tpcds"."sf1".store,
           "tpcds"."sf1".item
      WHERE ss_sold_date_sk=d_date_sk
        AND ss_item_sk=i_item_sk
        AND ss_store_sk = s_store_sk
        AND d_month_seq BETWEEN 1215 AND 1215+11
      GROUP BY rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
WHERE rk <= 100
ORDER BY i_category,
         i_class,
         i_brand,
         i_product_name,
         d_year,
         d_qoy,
         d_moy,
         s_store_id,
         sumsales,
         rk LIMIT 100;
