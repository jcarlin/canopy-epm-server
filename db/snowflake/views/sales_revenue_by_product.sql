CREATE VIEW sales_revenue_by_product AS
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
  r.rev AS bb_revenue
FROM (
  SELECT g2.goofy_d2_id AS d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,g6.goofy_d6_id AS d6_id,sum(r10) AS rev
  FROM root_11c1 r
  JOIN (
    SELECT m_id, max(ts) AS ts, d8_id, d3_id, d11_id, d4_id, d5_id, d2_id, d6_id, d9_id
    FROM root_11c1 r
    WHERE m_id = 3
      AND d9_id IN (194,195,196,198,199,200,202,203,204,206,207,208,192) --time_leaves (fiscal 2017)
      AND d2_id IN (43,64,85,106,127,148,169,190,211,232,253,274,295,316,337,358,379,400,421,442,463,484,505,526,547,568,589,610,631,652,673,694,715,736,757,778,799,820,841,862,883,904,925,946,967,988,1009,1030,1051,1072,1093,1114,1135,1156,1177,1198,1219,1240,1261,1282,1303,1324,1345,1366,1387,1408,1429,1450,1471,1492,1513,1534,1555,1576,1597,1618,1639,1660,1681,1702,1723,1744,1765,1786,1807,1828,1849,1870,1891,1912,1933,1954,1975,1996,2017,2038,2059,2080,2101,2122)   --tvs
      AND d3_id = 5   --lc
      AND d4_id = 52  --preliminary
      AND d5_id = 461 --4901b
      AND d8_id = 4   --booked_budget
      AND d11_id = 7  --periodic
    GROUP BY m_id, d8_id, d3_id, d11_id, d4_id, d5_id, d2_id, d6_id, d9_id
    ) m 
    ON r.m_id = m.m_id
      AND r.ts = m.ts
      AND r.d8_id = m.d8_id
      AND r.d3_id = m.d3_id
      AND r.d11_id = m.d11_id
      AND r.d4_id = m.d4_id
      AND r.d5_id = m.d5_id
      AND r.d2_id = m.d2_id
      AND r.d6_id = m.d6_id
      AND r.d9_id = m.d9_id
    JOIN grain_208 g2 ON r.d2_id = g2.d2_id --prod_shaper_tv
    JOIN grain_607 g6 ON r.d6_id = g6.d6_id --orga_region
  GROUP BY g2.goofy_d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,g6.goofy_d6_id
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