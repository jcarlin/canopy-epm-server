CREATE VIEW app_net_rev AS
SELECT
  r.d2_id,
  dim_2.d2_name AS product_id,
  r.d3_id,
  dim_3.d3_name AS currency_id,
  r.d4_id,
  dim_4.d4_name AS datasrc_id,
  r.d5_id,
  dim_5.d5_name AS objectcode_id,
  r.d8_id,
  dim_8.d8_name AS scenario_id,
  r.d9_id,
  dim_9.d9_name AS time_id,
  r.d11_id,
  dim_11.d11_name AS variation_id,
  r.d6_id,
  dim_6.d6_name AS organization_id,
  r.revenue AS app_net_rev
FROM (
  SELECT g2.goofy_d2_id AS d2_id,r.d3_id,g4.goofy_d4_id AS d4_id,g5.goofy_d5_id AS d5_id,r.d8_id,r.fp2_id AS d9_id,r.d11_id,g6.goofy_d6_id AS d6_id,sum(r10) AS revenue
  FROM (SELECT r.*, rank() over (partition by m_id, r.d8_id, r.d3_id, d11_id, d4_id, d5_id, d2_id, d6_id, d9_id ORDER BY ts DESC) AS _latest from root_11c1 r
        JOIN grain_301 g3 ON r.d3_id = g3.d3_id
        JOIN grain_801 g8 ON r.d8_id = g8.d8_id
        WHERE fp2_id IN (174,192,209)
        limit 1
       ) r
    JOIN grain_206 g2 ON r.d2_id = g2.d2_id
    JOIN grain_405 g4 ON r.d4_id = g4.d4_id
    JOIN grain_515 g5 ON r.d5_id = g5.d5_id
    JOIN grain_606 g6 ON r.d6_id = g6.d6_id
  WHERE _latest = 1
  GROUP BY g2.goofy_d2_id,r.d3_id,g4.goofy_d4_id,g5.goofy_d5_id,r.d8_id,r.fp2_id,r.d11_id,g6.goofy_d6_id
) r
JOIN dim_2 on r.d2_id = dim_2.d2_id
JOIN dim_3 on r.d3_id = dim_3.d3_id
JOIN dim_4 on r.d4_id = dim_4.d4_id
JOIN dim_5 on r.d5_id = dim_5.d5_id
JOIN dim_8 on r.d8_id = dim_8.d8_id
JOIN dim_9 on r.d9_id = dim_9.d9_id
JOIN dim_11 on r.d11_id = dim_11.d11_id
JOIN dim_6 on r.d6_id = dim_6.d6_id
;