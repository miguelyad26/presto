-- database: presto; groups: tpcds
SELECT *
FROM
  (SELECT avg(ss_list_price) b1_lp,
          count(ss_list_price) b1_cnt,
          count(DISTINCT ss_list_price) b1_cntd
   FROM "tpcds"."sf1".store_sales
   WHERE ss_quantity BETWEEN 0 AND 5
     AND (ss_list_price BETWEEN 17 AND 17+10
          OR ss_coupon_amt BETWEEN 12517 AND 12517+1000
          OR ss_wholesale_cost BETWEEN 4 AND 4+20)) b1,

  (SELECT avg(ss_list_price) b2_lp,
          count(ss_list_price) b2_cnt,
          count(DISTINCT ss_list_price) b2_cntd
   FROM "tpcds"."sf1".store_sales
   WHERE ss_quantity BETWEEN 6 AND 10
     AND (ss_list_price BETWEEN 16 AND 16+10
          OR ss_coupon_amt BETWEEN 10874 AND 10874+1000
          OR ss_wholesale_cost BETWEEN 6 AND 6+20)) b2,

  (SELECT avg(ss_list_price) b3_lp,
          count(ss_list_price) b3_cnt,
          count(DISTINCT ss_list_price) b3_cntd
   FROM "tpcds"."sf1".store_sales
   WHERE ss_quantity BETWEEN 11 AND 15
     AND (ss_list_price BETWEEN 183 AND 183+10
          OR ss_coupon_amt BETWEEN 4181 AND 4181+1000
          OR ss_wholesale_cost BETWEEN 23 AND 23+20)) b3,

  (SELECT avg(ss_list_price) b4_lp,
          count(ss_list_price) b4_cnt,
          count(DISTINCT ss_list_price) b4_cntd
   FROM "tpcds"."sf1".store_sales
   WHERE ss_quantity BETWEEN 16 AND 20
     AND (ss_list_price BETWEEN 138 AND 138+10
          OR ss_coupon_amt BETWEEN 13265 AND 13265+1000
          OR ss_wholesale_cost BETWEEN 20 AND 20+20)) b4,

  (SELECT avg(ss_list_price) b5_lp,
          count(ss_list_price) b5_cnt,
          count(DISTINCT ss_list_price) b5_cntd
   FROM "tpcds"."sf1".store_sales
   WHERE ss_quantity BETWEEN 21 AND 25
     AND (ss_list_price BETWEEN 158 AND 158+10
          OR ss_coupon_amt BETWEEN 6001 AND 6001+1000
          OR ss_wholesale_cost BETWEEN 75 AND 75+20)) b5,

  (SELECT avg(ss_list_price) b6_lp,
          count(ss_list_price) b6_cnt,
          count(DISTINCT ss_list_price) b6_cntd
   FROM "tpcds"."sf1".store_sales
   WHERE ss_quantity BETWEEN 26 AND 30
     AND (ss_list_price BETWEEN 175 AND 175+10
          OR ss_coupon_amt BETWEEN 4357 AND 4357+1000
          OR ss_wholesale_cost BETWEEN 36 AND 36+20)) b6 LIMIT 100;
