-- database: presto; groups: tpcds
SELECT i_item_id,
       s_state,
       grouping(s_state) g_state,
       avg(ss_quantity) agg1,
       avg(ss_list_price) agg2,
       avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
FROM "tpcds"."sf1".store_sales,
     "tpcds"."sf1".customer_demographics,
     "tpcds"."sf1".date_dim,
     "tpcds"."sf1".store,
     "tpcds"."sf1".item
WHERE ss_sold_date_sk = d_date_sk
  AND ss_item_sk = i_item_sk
  AND ss_store_sk = s_store_sk
  AND ss_cdemo_sk = cd_demo_sk
  AND cd_gender = 'M'
  AND cd_marital_status = 'M'
  AND cd_education_status = 'Advanced Degree'
  AND d_year = 1998
  AND s_state IN ('MI',
                  'LA',
                  'AL',
                  'MO',
                  'OH',
                  'GA')
GROUP BY ROLLUP (i_item_id,
                 s_state)
ORDER BY i_item_id,
         s_state LIMIT 100;
