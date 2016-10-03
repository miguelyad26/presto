-- database: presto; groups: tpcds
SELECT substr(w_warehouse_name,1,20),
       sm_type,
       web_name,
       sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days",
       sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
           AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days",
       sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
           AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days",
       sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
           AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days",
       sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM "tpcds"."sf1".web_sales,
     "tpcds"."sf1".warehouse,
     "tpcds"."sf1".ship_mode,
     "tpcds"."sf1".web_site,
     "tpcds"."sf1".date_dim
WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
  AND ws_ship_date_sk = d_date_sk
  AND ws_warehouse_sk = w_warehouse_sk
  AND ws_ship_mode_sk = sm_ship_mode_sk
  AND ws_web_site_sk = web_site_sk
GROUP BY substr(w_warehouse_name,1,20),
         sm_type,
         web_name
ORDER BY substr(w_warehouse_name,1,20),
         sm_type,
         web_name LIMIT 100;
