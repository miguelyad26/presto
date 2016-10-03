-- database: presto; groups: tpcds
WITH ss AS
  (SELECT s_store_sk,
          sum(ss_ext_sales_price) AS sales,
          sum(ss_net_profit) AS profit
   FROM "tpcds"."sf1".store_sales,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".store
   WHERE ss_sold_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-18' AS date) AND (cast('2000-08-18' AS date) + INTERVAL '30' DAY)
     AND ss_store_sk = s_store_sk
   GROUP BY s_store_sk) ,
     sr AS
  (SELECT s_store_sk,
          sum(sr_return_amt) AS RETURNS,
          sum(sr_net_loss) AS profit_loss
   FROM "tpcds"."sf1".store_returns,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".store
   WHERE sr_returned_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-18' AS date) AND (cast('2000-08-18' AS date) + INTERVAL '30' DAY)
     AND sr_store_sk = s_store_sk
   GROUP BY s_store_sk),
     cs AS
  (SELECT cs_call_center_sk,
          sum(cs_ext_sales_price) AS sales,
          sum(cs_net_profit) AS profit
   FROM "tpcds"."sf1".catalog_sales,
        "tpcds"."sf1".date_dim
   WHERE cs_sold_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-18' AS date) AND (cast('2000-08-18' AS date) + INTERVAL '30' DAY)
   GROUP BY cs_call_center_sk),
     cr AS
  (SELECT sum(cr_return_amount) AS RETURNS,
          sum(cr_net_loss) AS profit_loss
   FROM "tpcds"."sf1".catalog_returns,
        "tpcds"."sf1".date_dim
   WHERE cr_returned_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-18' AS date) AND (cast('2000-08-18' AS date) + INTERVAL '30' DAY) ),
     ws AS
  (SELECT wp_web_page_sk,
          sum(ws_ext_sales_price) AS sales,
          sum(ws_net_profit) AS profit
   FROM "tpcds"."sf1".web_sales,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".web_page
   WHERE ws_sold_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-18' AS date) AND (cast('2000-08-18' AS date) + INTERVAL '30' DAY)
     AND ws_web_page_sk = wp_web_page_sk
   GROUP BY wp_web_page_sk),
     wr AS
  (SELECT wp_web_page_sk,
          sum(wr_return_amt) AS RETURNS,
          sum(wr_net_loss) AS profit_loss
   FROM "tpcds"."sf1".web_returns,
        "tpcds"."sf1".date_dim,
        "tpcds"."sf1".web_page
   WHERE wr_returned_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-18' AS date) AND (cast('2000-08-18' AS date) + INTERVAL '30' DAY)
     AND wr_web_page_sk = wp_web_page_sk
   GROUP BY wp_web_page_sk)
SELECT channel ,
       id ,
       sum(sales) AS sales ,
       sum(RETURNS) AS RETURNS ,
       sum(profit) AS profit
FROM
  (SELECT '"tpcds"."sf1".store channel' AS channel ,
                             ss.s_store_sk AS id ,
                                              sales ,
                                              coalesce(RETURNS, 0) AS RETURNS , (profit - coalesce(profit_loss,0)) AS profit
   FROM ss
   LEFT JOIN sr ON ss.s_store_sk = sr.s_store_sk
   UNION ALL SELECT 'catalog channel' AS channel ,
                                         cs_call_center_sk AS id , sales , RETURNS , (profit - profit_loss) AS profit
   FROM cs ,
        cr
   UNION ALL SELECT 'web channel' AS channel ,
                                     ws.wp_web_page_sk AS id ,
                                                          sales ,
                                                          coalesce(RETURNS, 0) RETURNS , (profit - coalesce(profit_loss,0)) AS profit
   FROM ws
   LEFT JOIN wr ON ws.wp_web_page_sk = wr.wp_web_page_sk ) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel ,
         id LIMIT 100;
