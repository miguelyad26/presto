-- database: presto; groups: tpcds
SELECT promotions,
       total,
       cast(promotions AS decimal(15,4))/cast(total AS decimal(15,4))*100
FROM
  (SELECT sum(ss_ext_sales_price) promotions
   FROM "tpcds"."sf1".store_sales,
        "tpcds"."sf1".store,
        "tpcds"."sf1".promotion,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".customer,
        "tpcds"."sf1".customer_address,
        "tpcds"."sf1".item
   WHERE ss_sold_date_sk = d_date_sk
     AND ss_store_sk = s_store_sk
     AND ss_promo_sk = p_promo_sk
     AND ss_customer_sk= c_customer_sk
     AND ca_address_sk = c_current_addr_sk
     AND ss_item_sk = i_item_sk
     AND ca_gmt_offset = -6
     AND i_category = 'Jewelry'
     AND (p_channel_dmail = 'Y'
          OR p_channel_email = 'Y'
          OR p_channel_tv = 'Y')
     AND s_gmt_offset = -6
     AND d_year = 2000
     AND d_moy = 11) promotional_sales,

  (SELECT sum(ss_ext_sales_price) total
   FROM "tpcds"."sf1".store_sales,
        "tpcds"."sf1".store,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".customer,
        "tpcds"."sf1".customer_address,
        "tpcds"."sf1".item
   WHERE ss_sold_date_sk = d_date_sk
     AND ss_store_sk = s_store_sk
     AND ss_customer_sk= c_customer_sk
     AND ca_address_sk = c_current_addr_sk
     AND ss_item_sk = i_item_sk
     AND ca_gmt_offset = -6
     AND i_category = 'Jewelry'
     AND s_gmt_offset = -6
     AND d_year = 2000
     AND d_moy = 11) all_sales
ORDER BY promotions,
         total LIMIT 100;
