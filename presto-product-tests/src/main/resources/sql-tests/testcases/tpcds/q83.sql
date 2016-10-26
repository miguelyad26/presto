-- database: presto; groups: tpcds, quarantine
--- returns incorrect results: got BIGINT while expected DECIMAL
WITH sr_items AS
  (SELECT i_item_id item_id,
          sum(sr_return_quantity) sr_item_qty
   FROM "tpcds"."sf1".store_returns,
        "tpcds"."sf1".item,
        "tpcds"."sf1".date_dim
   WHERE sr_item_sk = i_item_sk
     AND d_date IN
       (SELECT d_date
        FROM "tpcds"."sf1".date_dim
        WHERE d_week_seq IN
            (SELECT d_week_seq
             FROM "tpcds"."sf1".date_dim
             WHERE d_date IN (cast('2000-06-30' AS DATE),
                              cast('2000-09-27' AS DATE),
                              cast('2000-11-17' AS DATE))))
     AND sr_returned_date_sk = d_date_sk
   GROUP BY i_item_id),

cr_items AS
  (SELECT i_item_id item_id,
          sum(cr_return_quantity) cr_item_qty
   FROM "tpcds"."sf1".catalog_returns,
        "tpcds"."sf1".item,
        "tpcds"."sf1".date_dim
   WHERE cr_item_sk = i_item_sk
     AND d_date IN
       (SELECT d_date
        FROM "tpcds"."sf1".date_dim
        WHERE d_week_seq IN
            (SELECT d_week_seq
             FROM "tpcds"."sf1".date_dim
             WHERE d_date IN (cast('2000-06-30' AS DATE),
                              cast('2000-09-27' AS DATE),
                              cast('2000-11-17' AS DATE))))
     AND cr_returned_date_sk = d_date_sk
   GROUP BY i_item_id),

wr_items AS
  (SELECT i_item_id item_id,
          sum(wr_return_quantity) wr_item_qty
   FROM "tpcds"."sf1".web_returns,
        "tpcds"."sf1".item,
        "tpcds"."sf1".date_dim
   WHERE wr_item_sk = i_item_sk
     AND d_date IN
       (SELECT d_date
        FROM "tpcds"."sf1".date_dim
        WHERE d_week_seq IN
            (SELECT d_week_seq
             FROM "tpcds"."sf1".date_dim
             WHERE d_date IN (cast('2000-06-30' AS DATE),
                              cast('2000-09-27' AS DATE),
                              cast('2000-11-17' AS DATE))))
     AND wr_returned_date_sk = d_date_sk
   GROUP BY i_item_id)

SELECT sr_items.item_id,
       sr_item_qty,
       sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev,
       cr_item_qty,
       cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev,
       wr_item_qty,
       wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev,
       (sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
FROM sr_items,
     cr_items,
     wr_items
WHERE sr_items.item_id=cr_items.item_id
  AND sr_items.item_id=wr_items.item_id
ORDER BY sr_items.item_id,
         sr_item_qty LIMIT 100;