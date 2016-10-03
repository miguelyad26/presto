-- database: presto; groups: tpcds
SELECT distinct(i_product_name)
FROM "tpcds"."sf1".item i1
WHERE i_manufact_id BETWEEN 751 AND 751+40
  AND
    (SELECT count(*) AS item_cnt
     FROM "tpcds"."sf1".item
     WHERE (i_manufact = i1.i_manufact
            AND ((i_category = 'Women'
                  AND (i_color = 'maroon'
                       OR i_color = 'orchid')
                  AND (i_units = 'Dram'
                       OR i_units = 'Unknown')
                  AND (i_size = 'small'
                       OR i_size = 'medium'))
                 OR (i_category = 'Women'
                     AND (i_color = 'tan'
                          OR i_color = 'olive')
                     AND (i_units = 'Box'
                          OR i_units = 'Case')
                     AND (i_size = 'N/A'
                          OR i_size = 'large'))
                 OR (i_category = 'Men'
                     AND (i_color = 'sienna'
                          OR i_color = 'cyan')
                     AND (i_units = 'Oz'
                          OR i_units = 'N/A')
                     AND (i_size = 'economy'
                          OR i_size = 'petite'))
                 OR (i_category = 'Men'
                     AND (i_color = 'drab'
                          OR i_color = 'blue')
                     AND (i_units = 'Tsp'
                          OR i_units = 'Each')
                     AND (i_size = 'small'
                          OR i_size = 'medium'))))
       OR (i_manufact = i1.i_manufact
           AND ((i_category = 'Women'
                 AND (i_color = 'rosy'
                      OR i_color = 'almond')
                 AND (i_units = 'Lb'
                      OR i_units = 'Gross')
                 AND (i_size = 'small'
                      OR i_size = 'medium'))
                OR (i_category = 'Women'
                    AND (i_color = 'misty'
                         OR i_color = 'violet')
                    AND (i_units = 'Tbl'
                         OR i_units = 'Pound')
                    AND (i_size = 'N/A'
                         OR i_size = 'large'))
                OR (i_category = 'Men'
                    AND (i_color = 'goldenrod'
                         OR i_color = 'indian')
                    AND (i_units = 'Bunch'
                         OR i_units = 'Carton')
                    AND (i_size = 'economy'
                         OR i_size = 'petite'))
                OR (i_category = 'Men'
                    AND (i_color = 'white'
                         OR i_color = 'green')
                    AND (i_units = 'Cup'
                         OR i_units = 'Gram')
                    AND (i_size = 'small'
                         OR i_size = 'medium'))))) > 0
ORDER BY i_product_name LIMIT 100;
