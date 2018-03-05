CREATE VIEW object_code_by_product_r4r6 AS
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
  a.revenue
FROM (
  SELECT 
    coalesce(r6.d2_id,r4.d2_id) AS d2_id,
    coalesce(r6.d3_id,r4.d3_id) AS d3_id,
    coalesce(r6.d4_id,r4.d4_id) AS d4_id,
    coalesce(r6.d5_id,r4.d5_id) AS d5_id,
    coalesce(r6.d8_id,r4.d8_id) AS d8_id,
    coalesce(r6.d9_id,r4.d9_id) AS d9_id,
    coalesce(r6.d11_id,r4.d11_id) AS d11_id,
    coalesce(r6.d6_id,r4.d6_id) AS d6_id,
    coalesce(r4,0) - coalesce(r6,0) AS revenue
  FROM (
    SELECT g2.goofy_d2_id AS d2_id,r.d3_id,g4.goofy_d4_id AS d4_id,g5.goofy_d5_id AS d5_id,r.d8_id,g9.goofy_d9_id AS d9_id,r.d11_id,g6.goofy_d6_id AS d6_id,sum(r4) AS r4
    FROM root_4 r
      JOIN grain_301 g3 ON r.d3_id = g3.d3_id
      JOIN grain_801 g8 ON r.d8_id = g8.d8_id
      JOIN grain_206 g2 ON r.d2_id = g2.d2_id
      JOIN grain_405 g4 ON r.d4_id = g4.d4_id
      JOIN grain_515 g5 ON r.d5_id = g5.d5_id
      JOIN grain_908 g9 ON r.d9_id = g9.d9_id
      JOIN grain_606 g6 ON r.d6_id = g6.d6_id
    WHERE active
    GROUP BY g2.goofy_d2_id,r.d3_id,g4.goofy_d4_id,g5.goofy_d5_id,r.d8_id,g9.goofy_d9_id,r.d11_id,g6.goofy_d6_id
  ) r4
  FULL OUTER JOIN (
    SELECT g2.goofy_d2_id AS d2_id,r.d3_id,g4.goofy_d4_id AS d4_id,g5.goofy_d5_id AS d5_id,r.d8_id,g9.goofy_d9_id AS d9_id,r.d11_id,g6.goofy_d6_id AS d6_id,sum(r6) AS r6
    FROM root_6 r
      JOIN grain_301 g3 ON r.d3_id = g3.d3_id
      JOIN grain_801 g8 ON r.d8_id = g8.d8_id
      JOIN grain_206 g2 ON r.d2_id = g2.d2_id
      JOIN grain_405 g4 ON r.d4_id = g4.d4_id
      JOIN grain_515 g5 ON r.d5_id = g5.d5_id
      JOIN grain_908 g9 ON r.d9_id = g9.d9_id
      JOIN grain_606 g6 ON r.d6_id = g6.d6_id
    WHERE active
    GROUP BY g2.goofy_d2_id,r.d3_id,g4.goofy_d4_id,g5.goofy_d5_id,r.d8_id,g9.goofy_d9_id,r.d11_id,g6.goofy_d6_id
  ) r6
  ON r4.d2_id = r6.d2_id AND r4.d3_id = r6.d3_id AND r4.d4_id = r6.d4_id AND r4.d5_id = r6.d5_id AND r4.d8_id = r6.d8_id AND r4.d9_id = r6.d9_id AND r4.d11_id = r6.d11_id AND r4.d6_id = r6.d6_id
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