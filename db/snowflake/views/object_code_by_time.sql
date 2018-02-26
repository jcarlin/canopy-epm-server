CREATE VIEW object_code_by_time AS
SELECT
  a.d2_id,
  dim_2.d2_name AS product_id,
  a.d3_id,
  dim_3.d3_name AS currency_id,
  a.d4_id,
  dim_4.d4_name AS datasrc_id,
  a.d5_id,
  dim_5.d5_name AS objectcode_id,
  a.d8_id,
  dim_8.d8_name AS scenario_id,
  a.d9_id,
  dim_9.d9_name AS time_id,
  a.d11_id,
  dim_11.d11_name AS variation_id,
  a.d6_id,
  dim_6.d6_name AS organization_id,
  r6 AS expense_balance
FROM (
  SELECT a.d2_id,a.d3_id,a.d4_id,a.d5_id,a.d8_id,a.d9_id,a.d11_id,a.d6_id,a.r6 AS r6
  FROM root_6 a
    JOIN grain_202 d2 ON a.d2_id = d2.d2_id
    JOIN grain_301 d3 ON a.d3_id = d3.d3_id
    JOIN grain_406 d4 ON a.d4_id = d4.d4_id
    JOIN grain_503 d5 ON a.d5_id = d5.d5_id
    JOIN grain_801 d8 ON a.d8_id = d8.d8_id
    JOIN grain_903 d9 ON a.d9_id = d9.d9_id
    JOIN grain_1102 d11 ON a.d11_id = d11.d11_id
    JOIN grain_602 d6 ON a.d6_id = d6.d6_id
  WHERE active
  ) a  
JOIN dim_2 on a.d2_id = dim_2.d2_id
JOIN dim_3 on a.d3_id = dim_3.d3_id
JOIN dim_4 on a.d4_id = dim_4.d4_id
JOIN dim_5 on a.d5_id = dim_5.d5_id
JOIN dim_8 on a.d8_id = dim_8.d8_id
JOIN dim_9 on a.d9_id = dim_9.d9_id
JOIN dim_11 on a.d11_id = dim_11.d11_id
JOIN dim_6 on a.d6_id = dim_6.d6_id
;