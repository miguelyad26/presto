-- database: presto; groups: tpcds
SELECT *
FROM
  (SELECT i_manager_id,
          sum(ss_sales_price) sum_sales,
          avg(sum(ss_sales_price)) over (partition BY i_manager_id) avg_monthly_sales
   FROM "tpcds"."sf1".item,
        "tpcds"."sf1".store_sales,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".store
   WHERE ss_item_sk = i_item_sk
     AND ss_sold_date_sk = d_date_sk
     AND ss_store_sk = s_store_sk
     AND d_month_seq IN (1216,
                         1216+1,
                         1216+2,
                         1216+3,
                         1216+4,
                         1216+5,
                         1216+6,
                         1216+7,
                         1216+8,
                         1216+9,
                         1216+10,
                         1216+11)
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('personal',
                           'portable',
                           'refernece',
                           'self-help')
           AND i_brand IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9')) or(i_category IN ('Women','Music','Men')
                                                       AND i_class IN ('accessories','classical','fragrances','pants')
                                                       AND i_brand IN ('amalgimporto #1','edu packscholar #1','exportiimporto #1', 'importoamalg #1')))
   GROUP BY i_manager_id,
            d_moy) tmp1
WHERE CASE
          WHEN avg_monthly_sales > 0 THEN ABS (sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY i_manager_id,
         avg_monthly_sales,
         sum_sales LIMIT 100;
