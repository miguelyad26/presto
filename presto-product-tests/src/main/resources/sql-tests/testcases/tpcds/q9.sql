-- database: presto; groups: tpcds
SELECT CASE
           WHEN
                  (SELECT count(*)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 1 AND 20) > 570145 THEN
                  (SELECT avg(ss_ext_sales_price)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 1 AND 20)
           ELSE
                  (SELECT avg(ss_net_profit)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 1 AND 20)
       END bucket1,
       CASE
           WHEN
                  (SELECT count(*)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 21 AND 40) > 898973 THEN
                  (SELECT avg(ss_ext_sales_price)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 21 AND 40)
           ELSE
                  (SELECT avg(ss_net_profit)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 21 AND 40)
       END bucket2,
       CASE
           WHEN
                  (SELECT count(*)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 41 AND 60) > 1637503 THEN
                  (SELECT avg(ss_ext_sales_price)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 41 AND 60)
           ELSE
                  (SELECT avg(ss_net_profit)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 41 AND 60)
       END bucket3,
       CASE
           WHEN
                  (SELECT count(*)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 61 AND 80) > 2652406 THEN
                  (SELECT avg(ss_ext_sales_price)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 61 AND 80)
           ELSE
                  (SELECT avg(ss_net_profit)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 61 AND 80)
       END bucket4,
       CASE
           WHEN
                  (SELECT count(*)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 81 AND 100) > 976428 THEN
                  (SELECT avg(ss_ext_sales_price)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 81 AND 100)
           ELSE
                  (SELECT avg(ss_net_profit)
                   FROM "tpcds"."sf1".store_sales
                   WHERE ss_quantity BETWEEN 81 AND 100)
       END bucket5
FROM "tpcds"."sf1".reason
WHERE r_reason_sk = 1 ;
