/*

SET search_path TO 'elt';

SELECT * FROM app_net_rev

SELECT objectcode_desc, app_net_rev FROM app_net_rev
WHERE 1=1
  AND d2_id = 4 --prod designated,5,home_audio  |  operating_budget,4,televisions
  AND d8_id = 4 --d8 scen_act_budget actual,booked_budget 3,4 (only 4 has data)
  AND d9_id = 192 --time time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
  AND d6_id = 3 -- d6 orga_shaper -- aa00;2;level_1_2_node -- aaaa;3;level_2_3_node -- paaa;83307;level_2_83307_node 
  AND d5_id IN (1313,1478,1528,1554) 
;
*/

SET search_path TO 'elt'; 

DROP VIEW IF EXISTS app_net_rev;
CREATE VIEW app_net_rev AS
SELECT
  d2_id,
  d2_name AS product_id,
  d3_id,
  d3_name AS currency_id,
  d4_id,
  d4_name AS datasrc_id,
  d5_id,
  d5_name AS objectcode_id,
  d5_desc AS objectcode_desc,
  d8_id,
  d8_name AS scenario_id,
  d9_id,
  d9_name AS time_id,
  d11_id,
  d11_name AS variation_id,
  d6_id,
  d6_name AS organization_id,
  to_char(trunc(a20),  'L999,999,999,999') AS app_net_rev
FROM (
  SELECT goofy_d2_id AS d2_id,d3_id,d4_id,goofy_d5_id AS d5_id,d8_id,d9_id,d11_id,goofy_d6_id AS d6_id,sum(a20) AS a20
  FROM app_20
  NATURAL JOIN grain_206 --d2 prod_shaper SELECT * FROM grain_206
  NATURAL JOIN grain_515 --d4 objc_shaper --objc_summary_report 
  NATURAL JOIN grain_606 --d6 orga_shaper -- aa00;2;level_1_2_node -- aaaa;3;level_2_3_node -- paaa;83307;level_2_83307_node 
  GROUP BY goofy_d2_id,d3_id,d4_id,goofy_d5_id,d8_id,d9_id,d11_id,goofy_d6_id
) a  
--   WHERE 1=1
--     AND d2_id = 4 --prod designated,5,home_audio  |  operating_budget,4,televisions
--     AND d8_id = 4 --d8 scen_act_budget actual,booked_budget 3,4 (only 4 has data)
--     AND d9_id = 192 --time time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
--     AND d6_id = 3 -- d6 orga_shaper -- aa00;2;level_1_2_node -- aaaa;3;level_2_3_node -- paaa;83307;level_2_83307_node 
NATURAL JOIN dim_2
NATURAL JOIN dim_3
NATURAL JOIN dim_4
NATURAL JOIN dim_5
NATURAL JOIN dim_8 
NATURAL JOIN dim_9
NATURAL JOIN dim_11
NATURAL JOIN dim_6
-- WHERE d5_id IN (1313,1478,1528,1554) 
ORDER BY objectcode_desc
;
/*

*/