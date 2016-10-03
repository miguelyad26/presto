-- database: presto; groups: tpcds
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          count(*) cnt
   FROM "tpcds"."sf1".store_sales,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".store,
        "tpcds"."sf1".household_demographics
   WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
     AND store_sales.ss_store_sk = store.s_store_sk
     AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     AND date_dim.d_dom BETWEEN 1 AND 2
     AND (household_demographics.hd_buy_potential = '>10000'
          OR household_demographics.hd_buy_potential = '5001-10000')
     AND household_demographics.hd_vehicle_count > 0
     AND CASE WHEN household_demographics.hd_vehicle_count > 0 THEN household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count ELSE NULL END > 1
     AND date_dim.d_year IN (1999,
                             1999+1,
                             1999+2)
     AND store.s_county IN ('Ziebach County',
                            'Fairfield County',
                            'Barrow County',
                            'Richland County')
   GROUP BY ss_ticket_number,
            ss_customer_sk) dj,
     "tpcds"."sf1".customer
WHERE ss_customer_sk = c_customer_sk
  AND cnt BETWEEN 1 AND 5
ORDER BY cnt DESC;
