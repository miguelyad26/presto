-- database: presto; groups: tpcds, quarantine
--- returns incorrect result (due some issue with CASE?)
SELECT *
FROM
  (SELECT i_manufact_id,
          sum(ss_sales_price) sum_sales,
          avg(sum(ss_sales_price)) over (partition BY i_manufact_id) avg_quarterly_sales
   FROM "tpcds"."sf1".item,
        "tpcds"."sf1".store_sales,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".store
   WHERE ss_item_sk = i_item_sk
     AND ss_sold_date_sk = d_date_sk
     AND ss_store_sk = s_store_sk
     AND d_month_seq IN (1200,
                         1200+1,
                         1200+2,
                         1200+3,
                         1200+4,
                         1200+5,
                         1200+6,
                         1200+7,
                         1200+8,
                         1200+9,
                         1200+10,
                         1200+11)
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('personal',
                           'portable',
                           'reference',
                           'self-help')
           AND i_brand IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9')) or(i_category IN ('Women','Music','Men')
                                                       AND i_class IN ('accessories','classical','fragrances','pants')
                                                       AND i_brand IN ('amalgimporto #1','edu packscholar #1','exportiimporto #1', 'importoamalg #1')))
   GROUP BY i_manufact_id,
            d_qoy) tmp1
WHERE CASE
          WHEN avg_quarterly_sales > 0 THEN ABS (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales
          ELSE NULL
      END > 0.1
ORDER BY avg_quarterly_sales,
         sum_sales,
         i_manufact_id LIMIT 100;