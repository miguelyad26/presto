-- database: presto; groups: tpcds
SELECT SUM (ss_quantity)
FROM "tpcds"."sf1".store_sales,
     "tpcds"."sf1".store,
     "tpcds"."sf1".customer_demographics,
     "tpcds"."sf1".customer_address,
     "tpcds"."sf1".date_dim
WHERE s_store_sk = ss_store_sk
  AND ss_sold_date_sk = d_date_sk
  AND d_year = 1998
  AND ((cd_demo_sk = ss_cdemo_sk
        AND cd_marital_status = 'U'
        AND cd_education_status = 'Unknown'
        AND ss_sales_price BETWEEN 100.00 AND 150.00)
       OR (cd_demo_sk = ss_cdemo_sk
           AND cd_marital_status = 'U'
           AND cd_education_status = 'Unknown'
           AND ss_sales_price BETWEEN 50.00 AND 100.00)
       OR (cd_demo_sk = ss_cdemo_sk
           AND cd_marital_status = 'U'
           AND cd_education_status = 'Unknown'
           AND ss_sales_price BETWEEN 150.00 AND 200.00))
  AND ((ss_addr_sk = ca_address_sk
        AND ca_country = 'United States'
        AND ca_state IN ('TN',
                         'IN',
                         'MN')
        AND ss_net_profit BETWEEN 0 AND 2000)
       OR (ss_addr_sk = ca_address_sk
           AND ca_country = 'United States'
           AND ca_state IN ('TX',
                            'CO',
                            'IA')
           AND ss_net_profit BETWEEN 150 AND 3000)
       OR (ss_addr_sk = ca_address_sk
           AND ca_country = 'United States'
           AND ca_state IN ('NY',
                            'MO',
                            'OK')
           AND ss_net_profit BETWEEN 50 AND 25000)) ;
