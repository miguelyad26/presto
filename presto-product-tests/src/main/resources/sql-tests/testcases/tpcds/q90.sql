-- database: presto; groups: tpcds
SELECT cast(amc AS decimal(15,4))/cast(pmc AS decimal(15,4)) am_pm_ratio
FROM
  (SELECT count(*) amc
   FROM "tpcds"."sf1".web_sales,
        "tpcds"."sf1".household_demographics,
        "tpcds"."sf1".time_dim,
        "tpcds"."sf1".web_page
   WHERE ws_sold_time_sk = time_dim.t_time_sk
     AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
     AND ws_web_page_sk = web_page.wp_web_page_sk
     AND time_dim.t_hour BETWEEN 10 AND 10+1
     AND household_demographics.hd_dep_count = 7
     AND web_page.wp_char_count BETWEEN 5000 AND 5200) AT,

  (SELECT count(*) pmc
   FROM "tpcds"."sf1".web_sales,
        "tpcds"."sf1".household_demographics,
        "tpcds"."sf1".time_dim,
        "tpcds"."sf1".web_page
   WHERE ws_sold_time_sk = time_dim.t_time_sk
     AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
     AND ws_web_page_sk = web_page.wp_web_page_sk
     AND time_dim.t_hour BETWEEN 16 AND 16+1
     AND household_demographics.hd_dep_count = 7
     AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER BY am_pm_ratio LIMIT 100;
