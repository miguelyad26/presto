-- database: presto; groups: tpcds
SELECT substr(w_warehouse_name,1,20) ,
       sm_type ,
       cc_name ,
       sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days",
       sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 30)
           AND (cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days",
       sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 60)
           AND (cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days",
       sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 90)
           AND (cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days",
       sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM "tpcds"."sf1".catalog_sales ,
     "tpcds"."sf1".warehouse ,
     "tpcds"."sf1".ship_mode ,
     "tpcds"."sf1".call_center ,
     "tpcds"."sf1".date_dim
WHERE d_month_seq BETWEEN 1213 AND 1213 + 11
  AND cs_ship_date_sk = d_date_sk
  AND cs_warehouse_sk = w_warehouse_sk
  AND cs_ship_mode_sk = sm_ship_mode_sk
  AND cs_call_center_sk = cc_call_center_sk
GROUP BY substr(w_warehouse_name,1,20) ,
         sm_type ,
         cc_name
ORDER BY substr(w_warehouse_name,1,20) ,
         sm_type ,
         cc_name LIMIT 100;
