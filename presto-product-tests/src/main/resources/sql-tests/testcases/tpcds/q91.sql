-- database: presto; groups: tpcds
SELECT cc_call_center_id call_center,
       cc_name call_center_name,
       cc_manager manager,
       sum(cr_net_loss) returns_loss
FROM "tpcds"."sf1".call_center,
     "tpcds"."sf1".catalog_returns,
     "tpcds"."sf1".date_dim,
     "tpcds"."sf1".customer,
     "tpcds"."sf1".customer_address,
     "tpcds"."sf1".customer_demographics,
     "tpcds"."sf1".household_demographics
WHERE cr_call_center_sk = cc_call_center_sk
  AND cr_returned_date_sk = d_date_sk
  AND cr_returning_customer_sk= c_customer_sk
  AND cd_demo_sk = c_current_cdemo_sk
  AND hd_demo_sk = c_current_hdemo_sk
  AND ca_address_sk = c_current_addr_sk
  AND d_year = 2002
  AND d_moy = 12
  AND ((cd_marital_status = 'M'
        AND cd_education_status = 'Unknown') or(cd_marital_status = 'W'
                                                AND cd_education_status = 'Advanced Degree'))
  AND hd_buy_potential LIKE '501-1000%'
  AND ca_gmt_offset = -6
GROUP BY cc_call_center_id,
         cc_name,
         cc_manager,
         cd_marital_status,
         cd_education_status
ORDER BY sum(cr_net_loss) DESC;
