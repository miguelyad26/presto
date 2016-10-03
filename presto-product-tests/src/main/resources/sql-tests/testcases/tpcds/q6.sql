-- database: presto; groups: tpcds
SELECT a.ca_state STATE,
                  count(*) cnt
FROM "tpcds"."sf1".customer_address a,
     "tpcds"."sf1".customer c,
     "tpcds"."sf1".store_sales s,
     "tpcds"."sf1".date_dim d,
     "tpcds"."sf1".item i
WHERE a.ca_address_sk = c.c_current_addr_sk
  AND c.c_customer_sk = s.ss_customer_sk
  AND s.ss_sold_date_sk = d.d_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND d.d_month_seq =
    (SELECT DISTINCT (d_month_seq)
     FROM "tpcds"."sf1".date_dim
     WHERE d_year = 1998
       AND d_moy = 1)
  AND i.i_current_price > 1.2 *
    (SELECT avg(j.i_current_price)
     FROM "tpcds"."sf1".item j
     WHERE j.i_category = i.i_category)
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt LIMIT 100;
