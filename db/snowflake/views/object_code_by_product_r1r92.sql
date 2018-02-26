CREATE VIEW object_code_by_product_r1r92 AS
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
  r.revenue
FROM (
  SELECT goofy_d2_id AS d2_id,d3_id,goofy_d4_id AS d4_id,goofy_d5_id AS d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,goofy_d6_id AS d6_id,sum(revenue) AS revenue
  FROM (
    SELECT
      coalesce(r1.d2_id,r.d2_id) AS d2_id,
      coalesce(r1.d3_id,r.d3_id) AS d3_id,
      coalesce(r1.d4_id,r.d4_id) AS d4_id,
      coalesce(r1.d5_id,r.d5_id) AS d5_id,
      coalesce(r1.d8_id,r.d8_id) AS d8_id,
      coalesce(r1.d9_id,r.d9_id) AS d9_id,
      coalesce(r1.d11_id,r.d11_id) AS d11_id,
      coalesce(r1.d6_id,r.d6_id) AS d6_id,
      coalesce(r1,0) + coalesce(r2,0) + coalesce(r3,0) + coalesce(r4,0) - coalesce(r5,0) - coalesce(r6,0) - coalesce(r7,0) - coalesce(r8,0) + coalesce(r9,0) AS revenue
    FROM (
      SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,sum(r1) AS r1
      FROM root_1 r
      WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
      GROUP BY r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id
      ) r1
    FULL OUTER JOIN (
      SELECT 
        coalesce(r2.d2_id,r.d2_id) AS d2_id,
        coalesce(r2.d3_id,r.d3_id) AS d3_id,
        coalesce(r2.d4_id,r.d4_id) AS d4_id,
        coalesce(r2.d5_id,r.d5_id) AS d5_id,
        coalesce(r2.d8_id,r.d8_id) AS d8_id,
        coalesce(r2.d9_id,r.d9_id) AS d9_id,
        coalesce(r2.d11_id,r.d11_id) AS d11_id,
        coalesce(r2.d6_id,r.d6_id) AS d6_id,
        r2,r3,r4,r5,r6,r7,r8,r9
      FROM (
        SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,r2
        FROM root_2 r
        WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
        ) r2
      FULL OUTER JOIN (
        SELECT 
          coalesce(r3.d2_id,r.d2_id) AS d2_id,
          coalesce(r3.d3_id,r.d3_id) AS d3_id,
          coalesce(r3.d4_id,r.d4_id) AS d4_id,
          coalesce(r3.d5_id,r.d5_id) AS d5_id,
          coalesce(r3.d8_id,r.d8_id) AS d8_id,
          coalesce(r3.d9_id,r.d9_id) AS d9_id,
          coalesce(r3.d11_id,r.d11_id) AS d11_id,
          coalesce(r3.d6_id,r.d6_id) AS d6_id,
          r3,r4,r5,r6,r7,r8,r9
        FROM (
          SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,r3
          FROM root_3 r
          WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
        ) r3
        FULL OUTER JOIN (
          SELECT 
            coalesce(r4.d2_id,r.d2_id) AS d2_id,
            coalesce(r4.d3_id,r.d3_id) AS d3_id,
            coalesce(r4.d4_id,r.d4_id) AS d4_id,
            coalesce(r4.d5_id,r.d5_id) AS d5_id,
            coalesce(r4.d8_id,r.d8_id) AS d8_id,
            coalesce(r4.d9_id,r.d9_id) AS d9_id,
            coalesce(r4.d11_id,r.d11_id) AS d11_id,
            coalesce(r4.d6_id,r.d6_id) AS d6_id,
            r4,r5,r6,r7,r8,r9
          FROM (
            SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,r4
            FROM root_4 r
            WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
            ) r4
          FULL OUTER JOIN (
            SELECT 
              coalesce(r5.d2_id,r.d2_id) AS d2_id,
              coalesce(r5.d3_id,r.d3_id) AS d3_id,
              coalesce(r5.d4_id,r.d4_id) AS d4_id,
              coalesce(r5.d5_id,r.d5_id) AS d5_id,
              coalesce(r5.d8_id,r.d8_id) AS d8_id,
              coalesce(r5.d9_id,r.d9_id) AS d9_id,
              coalesce(r5.d11_id,r.d11_id) AS d11_id,
              coalesce(r5.d6_id,r.d6_id) AS d6_id,
              r5,r6,r7,r8,r9
            FROM (
              SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,r5
              FROM root_5 r
              WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
              ) r5
            FULL OUTER JOIN (
              SELECT 
                coalesce(r6.d2_id,r.d2_id) AS d2_id,
                coalesce(r6.d3_id,r.d3_id) AS d3_id,
                coalesce(r6.d4_id,r.d4_id) AS d4_id,
                coalesce(r6.d5_id,r.d5_id) AS d5_id,
                coalesce(r6.d8_id,r.d8_id) AS d8_id,
                coalesce(r6.d9_id,r.d9_id) AS d9_id,
                coalesce(r6.d11_id,r.d11_id) AS d11_id,
                coalesce(r6.d6_id,r.d6_id) AS d6_id,
                r6,r7,r8,r9
              FROM (
                SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,r6
                FROM root_6 r
                WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
              ) r6
              FULL OUTER JOIN (
                SELECT 
                  coalesce(r7.d2_id,r.d2_id) AS d2_id,
                  coalesce(r7.d3_id,r.d3_id) AS d3_id,
                  coalesce(r7.d4_id,r.d4_id) AS d4_id,
                  coalesce(r7.d5_id,r.d5_id) AS d5_id,
                  coalesce(r7.d8_id,r.d8_id) AS d8_id,
                  coalesce(r7.d9_id,r.d9_id) AS d9_id,
                  coalesce(r7.d11_id,r.d11_id) AS d11_id,
                  coalesce(r7.d6_id,r.d6_id) AS d6_id,
                  r7,r8,r9
                FROM (
                  SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,r7
                  FROM root_7 r
                  WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
                  ) r7
                FULL OUTER JOIN (
                  SELECT 
                    coalesce(r8.d2_id,r9.d2_id) AS d2_id,
                    coalesce(r8.d3_id,r9.d3_id) AS d3_id,
                    coalesce(r8.d4_id,r9.d4_id) AS d4_id,
                    coalesce(r8.d5_id,r9.d5_id) AS d5_id,
                    coalesce(r8.d8_id,r9.d8_id) AS d8_id,
                    coalesce(r8.d9_id,r9.d9_id) AS d9_id,
                    coalesce(r8.d11_id,r9.d11_id) AS d11_id,
                    coalesce(r8.d6_id,r9.d6_id) AS d6_id,
                    r8,r9
                  FROM (
                    SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,sum(r8) AS r8
                    FROM root_8 r
                    WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
                    GROUP BY r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id
                    ) r8
                  FULL OUTER JOIN (
                    SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,sum(r9) AS r9
                    FROM root_9 r
                    WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
                    GROUP BY r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id
                    ) r9
                    ON r8.d2_id = r9.d2_id AND r8.d3_id = r9.d3_id AND r8.d4_id = r9.d4_id AND r8.d5_id = r9.d5_id AND r8.d8_id = r9.d8_id AND r8.d9_id = r9.d9_id AND r8.d11_id = r9.d11_id AND r8.d6_id = r9.d6_id
                  ) r
                  ON r7.d2_id = r.d2_id AND r7.d3_id = r.d3_id AND r7.d4_id = r.d4_id AND r7.d5_id = r.d5_id AND r7.d8_id = r.d8_id AND r7.d9_id = r.d9_id AND r7.d11_id = r.d11_id AND r7.d6_id = r.d6_id
                ) r
                ON r6.d2_id = r.d2_id AND r6.d3_id = r.d3_id AND r6.d4_id = r.d4_id AND r6.d5_id = r.d5_id AND r6.d8_id = r.d8_id AND r6.d9_id = r.d9_id AND r6.d11_id = r.d11_id AND r6.d6_id = r.d6_id
              ) r
              ON r5.d2_id = r.d2_id AND r5.d3_id = r.d3_id AND r5.d4_id = r.d4_id AND r5.d5_id = r.d5_id AND r5.d8_id = r.d8_id AND r5.d9_id = r.d9_id AND r5.d11_id = r.d11_id AND r5.d6_id = r.d6_id
            ) r
            ON r4.d2_id = r.d2_id AND r4.d3_id = r.d3_id AND r4.d4_id = r.d4_id AND r4.d5_id = r.d5_id AND r4.d8_id = r.d8_id AND r4.d9_id = r.d9_id AND r4.d11_id = r.d11_id AND r4.d6_id = r.d6_id
          ) r
          ON r3.d2_id = r.d2_id AND r3.d3_id = r.d3_id AND r3.d4_id = r.d4_id AND r3.d5_id = r.d5_id AND r3.d8_id = r.d8_id AND r3.d9_id = r.d9_id AND r3.d11_id = r.d11_id AND r3.d6_id = r.d6_id
        ) r
        ON r2.d2_id = r.d2_id AND r2.d3_id = r.d3_id AND r2.d4_id = r.d4_id AND r2.d5_id = r.d5_id AND r2.d8_id = r.d8_id AND r2.d9_id = r.d9_id AND r2.d11_id = r.d11_id AND r2.d6_id = r.d6_id
      ) r
      ON r1.d2_id = r.d2_id AND r1.d3_id = r.d3_id AND r1.d4_id = r.d4_id AND r1.d5_id = r.d5_id AND r1.d8_id = r.d8_id AND r1.d9_id = r.d9_id AND r1.d11_id = r.d11_id AND r1.d6_id = r.d6_id
    ) r
  JOIN grain_206 g2 ON r.d2_id = g2.d2_id
  JOIN grain_405 g4 ON r.d4_id = g4.d4_id
  JOIN grain_515 g5 ON r.d5_id = g5.d5_id
  JOIN grain_908 g9 ON r.d9_id = g9.d9_id
  JOIN grain_606 g6 ON r.d6_id = g6.d6_id
  GROUP BY goofy_d2_id,d3_id,goofy_d4_id,goofy_d5_id,d8_id,goofy_d9_id,d11_id,goofy_d6_id
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