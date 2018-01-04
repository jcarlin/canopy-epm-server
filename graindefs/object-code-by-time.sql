/*

select * from <manifest_name converted to underscores>_<rowIndex><colIndex><rowDepth><colDepth>

Example
SELECT * FROM object_code_by_time
WHERE d2_id = 4 AND d3_id = 5 AND d4_id = 59 AND d8_id = 3 AND d11_id = 7 AND d6_id = 99460;



*/

DROP VIEW IF EXISTS object_code_by_time;
CREATE VIEW object_code_by_time AS
SELECT
  d2_id,
  d2_name AS product_id,
--   d2_desc AS product_desc,
  d3_id,
  d3_name AS currency_id,
--   d3_desc AS currency_desc,
  d4_id,
  d4_name AS datasrc_id,
--   d4_desc AS datasrc_desc,
  d5_id,
  d5_name AS objectcode_id,
--   d5_desc AS objectcode_desc,
  d8_id,
  d8_name AS scenario_id,
--   d8_desc AS scenario_desc,
  d9_id,
  d9_name AS time_id,
--   d9_desc AS time_desc,
  d11_id,
  d11_name AS variation_id,
--   d11_desc AS variation_desc,
  d6_id,
  d6_name AS organization_id,
--   d6_desc AS organization_desc,
  to_char(trunc(r6),  'L999,999,999,999') AS expense_balance
FROM (

-- SELECT * FROM dim_4 WHERE d4_id IN (59,62)  --edw_ga_activity has data; edw_gl_activity does not

/* Pulling the data entry data is longer than the the agg data, sad!
  SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r6 AS r6
  FROM root_6 NATURAL JOIN grain_503 NATURAL JOIN grain_903
  WHERE d2_id = 4 AND d3_id = 5 AND d4_id = 59 AND d8_id = 3 AND d11_id = 7 AND d6_id = 99460;
*/
-- SELECT count(*) FROM (
-- SELECT * FROM (
-- SELECT DISTINCT d4_id FROM (
  SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r6 AS r6
  FROM root_6
    NATURAL JOIN grain_202 --d2 SELECT * FROM grain_prod_televisions --4,televisions
    NATURAL JOIN grain_301 --d3 SELECT * FROM grain_curr_lc --5,lc
--     NATURAL JOIN grain_402 --d4 SELECT * FROM grain_dats_edw_gl_activity --62,edw_gl_activity
    NATURAL JOIN grain_406 --d4 SELECT * FROM grain_dats_edw_ga_activity --59,edw_ga_activity
    NATURAL JOIN grain_503 --d5 SELECT * FROM grain_objc_leaves --
    NATURAL JOIN grain_801 --d8 SELECT * FROM grain_scen_act_budget --actual,booked_budget 3,4 (only 4 has data)
    NATURAL JOIN grain_903 --d9 SELECT * FROM grain_time_leaves --
    NATURAL JOIN grain_1102 --d11 SELECT * FROM grain_vari_periodic --7,periodic
    NATURAL JOIN grain_602 --d6 SELECT * FROM grain_orga_level_10_99460_leaf --99460,level_10_99460_leaf
  WHERE active
-- ) a  
-- WHERE d2_id = 4 AND d3_id = 5 AND d4_id = 59 AND d8_id = 3 AND d11_id = 7 AND d6_id = 99460;

  ) a  
--   WHERE 1=1  SELECT * FROM s_fact
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
-- WHERE d2_id = 4 AND d3_id = 5 AND d4_id = 59 AND d8_id = 3 AND d11_id = 7 AND d6_id = 99460
-- ORDER BY objectcode_id
;
/*
-- WITH core AS (SELECT 4 AS d2_id,5 AS d3_id,59 AS d4_id,1001 AS d5_id,3 AS d8_id,195 AS d9_id,7 AS d11_id,99460 AS d6_id)
-- SELECT * FROM object_code_by_time_00 NATURAL JOIN core
SELECT * FROM object_code_by_time_00 WHERE d2_id = 4 AND d3_id = 5 AND d4_id = 59 AND d8_id = 3 AND d11_id = 7 AND d6_id = 99460;

There are 4 regions to the query, so 4 unions:

  SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r6 AS r6
  FROM root_6
    NATURAL JOIN grain_202 --d2 SELECT * FROM grain_prod_televisions --4,televisions
    NATURAL JOIN grain_301 --d3 SELECT * FROM grain_curr_lc --5,lc
--     NATURAL JOIN grain_402 --d4 SELECT * FROM grain_dats_edw_gl_activity --62,edw_gl_activity
    NATURAL JOIN grain_406 --d4 SELECT * FROM grain_dats_edw_ga_activity --59,edw_ga_activity
    NATURAL JOIN grain_503 --d5 SELECT * FROM grain_objc_leaves --
    NATURAL JOIN grain_801 --d8 SELECT * FROM grain_scen_act_budget --actual,booked_budget 3,4 (only 4 has data)
    NATURAL JOIN grain_903 --d9 SELECT * FROM grain_time_leaves --
    NATURAL JOIN grain_1102 --d11 SELECT * FROM grain_vari_periodic --7,periodic
    NATURAL JOIN grain_602 --d6 SELECT * FROM grain_orga_level_10_99460_leaf --99460,level_10_99460_leaf


  SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r6 AS r6
  FROM root_6
    NATURAL JOIN grain_202 --d2 SELECT * FROM grain_prod_televisions --4,televisions
    NATURAL JOIN grain_301 --d3 SELECT * FROM grain_curr_lc --5,lc
--     NATURAL JOIN grain_402 --d4 SELECT * FROM grain_dats_edw_gl_activity --62,edw_gl_activity
    NATURAL JOIN grain_406 --d4 SELECT * FROM grain_dats_edw_ga_activity --59,edw_ga_activity
    NATURAL JOIN grain_503 --d5 SELECT * FROM grain_objc_leaves --
    NATURAL JOIN grain_801 --d8 SELECT * FROM grain_scen_act_budget --actual,booked_budget 3,4 (only 4 has data)
    NATURAL JOIN grain_903 --d9 SELECT * FROM grain_time_leaves --
    NATURAL JOIN grain_1102 --d11 SELECT * FROM grain_vari_periodic --7,periodic
    NATURAL JOIN grain_602 --d6 SELECT * FROM grain_orga_level_10_99460_leaf --99460,level_10_99460_leaf
*/