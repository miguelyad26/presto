-- database: presto; groups: tpcds
SELECT * from
  (SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, sum(ss_sales_price) sum_sales, avg(sum(ss_sales_price)) over (partition BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
   FROM "tpcds"."sf1".item, "tpcds"."sf1".store_sales, "tpcds"."sf1".date_dim, "tpcds"."sf1".store
   WHERE ss_item_sk = i_item_sk
     AND ss_sold_date_sk = d_date_sk
     AND ss_store_sk = s_store_sk
     AND d_year IN (1999)
     AND ((i_category IN ('Shoes','Jewelry','Men')
           AND i_class IN ('kids','pendants','shirts') )
          OR (i_category IN ('Children','Books','Electronics')
              AND i_class IN ('newborn','self-help','cameras')))
   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy) tmp1
WHERE CASE
          WHEN (avg_monthly_sales <> 0) THEN (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
          ELSE NULL
      END > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         s_store_name LIMIT 100;
