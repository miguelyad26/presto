-- database: presto; groups: tpcds
WITH ss AS
  (SELECT i_item_id,
          sum(ss_ext_sales_price) total_sales
   FROM "tpcds"."sf1".store_sales,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".customer_address,
        "tpcds"."sf1".item
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM "tpcds"."sf1".item
        WHERE i_color IN ('olive',
                          'slate',
                          'ghost'))
     AND ss_item_sk = i_item_sk
     AND ss_sold_date_sk = d_date_sk
     AND d_year = 2002
     AND d_moy = 6
     AND ss_addr_sk = ca_address_sk
     AND ca_gmt_offset = -7
   GROUP BY i_item_id),
     cs AS
  (SELECT i_item_id,
          sum(cs_ext_sales_price) total_sales
   FROM "tpcds"."sf1".catalog_sales,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".customer_address,
        "tpcds"."sf1".item
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM "tpcds"."sf1".item
        WHERE i_color IN ('olive',
                          'slate',
                          'ghost'))
     AND cs_item_sk = i_item_sk
     AND cs_sold_date_sk = d_date_sk
     AND d_year = 2002
     AND d_moy = 6
     AND cs_bill_addr_sk = ca_address_sk
     AND ca_gmt_offset = -7
   GROUP BY i_item_id),
     ws AS
  (SELECT i_item_id,
          sum(ws_ext_sales_price) total_sales
   FROM "tpcds"."sf1".web_sales,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".customer_address,
        "tpcds"."sf1".item
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM "tpcds"."sf1".item
        WHERE i_color IN ('olive',
                          'slate',
                          'ghost'))
     AND ws_item_sk = i_item_sk
     AND ws_sold_date_sk = d_date_sk
     AND d_year = 2002
     AND d_moy = 6
     AND ws_bill_addr_sk = ca_address_sk
     AND ca_gmt_offset = -7
   GROUP BY i_item_id)
SELECT i_item_id,
       sum(total_sales) total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) tmp1
GROUP BY i_item_id
ORDER BY total_sales LIMIT 100;
