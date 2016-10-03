-- database: presto; groups: tpcds
WITH ssci AS
  (SELECT ss_customer_sk customer_sk ,
          ss_item_sk item_sk
   FROM "tpcds"."sf1".store_sales,
        "tpcds"."sf1".date_dim
   WHERE ss_sold_date_sk = d_date_sk
     AND d_month_seq BETWEEN 1186 AND 1186 + 11
   GROUP BY ss_customer_sk ,
            ss_item_sk),
     csci AS
  ( SELECT cs_bill_customer_sk customer_sk ,
           cs_item_sk item_sk
   FROM "tpcds"."sf1".catalog_sales,
        "tpcds"."sf1".date_dim
   WHERE cs_sold_date_sk = d_date_sk
     AND d_month_seq BETWEEN 1186 AND 1186 + 11
   GROUP BY cs_bill_customer_sk ,
            cs_item_sk)
SELECT sum(CASE WHEN ssci.customer_sk IS NOT NULL
           AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) store_only ,
       sum(CASE WHEN ssci.customer_sk IS NULL
           AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) catalog_only ,
       sum(CASE WHEN ssci.customer_sk IS NOT NULL
           AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) store_and_catalog
FROM ssci
FULL OUTER JOIN csci ON (ssci.customer_sk=csci.customer_sk
                         AND ssci.item_sk = csci.item_sk) LIMIT 100;
