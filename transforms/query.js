const getDimensionIdSql = dimensions => {
  const subSelects = dimensions.map(dim => {
    return `
      (SELECT d${dim.id}_id
      FROM dim_${dim.id}
      WHERE d${dim.id}_name = '${dim.member}') AS "d${dim.id}_id"`;
  });

  const sql = `
    SELECT * 
    FROM (SELECT ${subSelects.join(',')}) a;
  `;

  // console.log('getDimensionIdSql: ', sql);
  return sql;
};

// Query to build table data for grid
const querySql = (params) => {
  const sql = `
    SELECT 
      ${params.dimensions},
      ${params.metrics}
    FROM ${params.tableName}
    WHERE ${params.filterStatements};
  `;

  // console.log('querySql: ', sql);
  return sql;
};

// Update (manifest) app table 
const updateAppTableSql = (params) => {
  const sql = `
    UPDATE ${params.tableName} 
    SET "${params.metric}" = ${params.newValue}
    WHERE ${params.filterStatements};
  `;

  // console.log('updateAppTableSql: ', sql);
  return sql;
};

/**
 * Queries for updating
 */
const unnestFactTableKeySql = factId => {
  const sql = `
    SELECT *
    FROM (
      SELECT unnest(fact_key) AS dim_id 
      FROM s_fact 
      WHERE fact_id = ${factId}
    ) a
      NATURAL JOIN s_dim
    ORDER BY dim_byte, dim_id;
  `;

  console.log('unnestFactTableKeySql: ', sql);
  return sql;
};

const deactivateSql = (params) => {
  const sql = `
    UPDATE ${params.tableName} u
    SET active = false
    WHERE active = true AND ${params.filterStatements};
  `;

  console.log('deactivateSql: ', sql);
  return sql;
};

const insertSql = (params) => {
  const sql = `
    INSERT INTO ${params.tableName} (
      ${params.dimIdColumns}, 
      r${params.factId}, 
      r${params.factId}_ord, 
      active)
    VALUES (
      ${params.dimIdValues},
      ${params.newValue}::DOUBLE PRECISION, 
      (SELECT max(r${params.factId}_ord) + 1 FROM ${params.tableName}),
      true
    );
  `;
  
  console.log("insertSql: ", sql);
  return sql;
};

const sfInsertSql = (params) => {
  const sql = `
    INSERT INTO root_11c1 (m_id, ${params.dimIdColumns}, r10, fp2_id, fp1_id, cp2_id, cp1_id) 
    SELECT (SELECT fact_id FROM s_fact WHERE fact_name = r.metric), ${params.dimRIdColumns}, r3, fp2.d9h2_parent_id AS fp2_id, p1.d9h2_parent_id AS fp1_id, cp2.d9h3_parent_id AS cp2_id, p1.d9h3_parent_id AS cp1_id
    FROM (
      SELECT 'bb_revenue' AS metric, ${params.dimRIdValues}, ${params.newValue} AS r3) r
    JOIN dim_9 p1 ON p1.d9_id = r.d9_id
    LEFT OUTER JOIN dim_9 fp2 ON p1.d9h2_parent_id = fp2.d9_id
    LEFT OUTER JOIN dim_9 cp2 ON p1.d9h3_parent_id = cp2.d9_id;`;

  console.log('sfInsertSql: ', sql);
  return sql;
};

const updateBranch15NatJoinSql = (params) => {
  const sql = `
    UPDATE branch_15 u
    SET b15 = new_b15
    FROM (
      WITH keystone AS (SELECT ${params.dimIdColumns} FROM ${params.tableName} WHERE active = true AND ${params.filterStatements})
      SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,
      coalesce(r1,0) + coalesce(r2,0) + coalesce(r3,0) + coalesce(r4,0) - coalesce(r5,0) - coalesce(r6,0) - coalesce(r7,0) - coalesce(r8,0) + coalesce(r9,0) AS new_b15
      FROM (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,sum(r1) AS r1
        FROM root_1 NATURAL JOIN keystone WHERE active
        GROUP BY d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id
        ) r1
      NATURAL FULL OUTER JOIN (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r2
        FROM root_2 NATURAL JOIN keystone WHERE active
        ) r2
      NATURAL FULL OUTER JOIN (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r3
        FROM root_3 NATURAL JOIN keystone WHERE active
      ) r3
      NATURAL FULL OUTER JOIN (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r4
        FROM root_4 NATURAL JOIN keystone WHERE active
        ) r4
      NATURAL FULL OUTER JOIN (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r5
        FROM root_5 NATURAL JOIN keystone WHERE active
        ) r5
      NATURAL FULL OUTER JOIN (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r6
        FROM root_6 NATURAL JOIN keystone WHERE active
      ) r6
      NATURAL FULL OUTER JOIN (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r7
        FROM root_7 NATURAL JOIN keystone WHERE active
        ) r7
      NATURAL FULL OUTER JOIN (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,sum(r8) AS r8
        FROM root_8 NATURAL JOIN keystone WHERE active
        GROUP BY d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id
        ) r8
      NATURAL FULL OUTER JOIN (
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,sum(r9) AS r9
        FROM root_9 NATURAL JOIN keystone WHERE active
        GROUP BY d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id
        ) r9
    ) a
    WHERE u.d2_id = a.d2_id
      AND u.d3_id = a.d3_id
      AND u.d4_id = a.d4_id
      AND u.d5_id = a.d5_id
      AND u.d8_id = a.d8_id
      AND u.d9_id = a.d9_id
      AND u.d11_id = a.d11_id
      AND u.d6_id = a.d6_id
    RETURNING u.*;
  `;

  console.log('updateBranch15NatJoinSql: ', sql);
  return sql;
};

const updateApp20NatJoinSql = (params) => {
  const sql = `    
    UPDATE app_20 u
    SET a20 = new_a20
    FROM (
        --elt is modified to add the keystone
        WITH keystone AS (
          WITH this_key AS (SELECT ${params.dimIdColumns} FROM ${params.tableName} WHERE active = true AND ${params.filterStatements})
          SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id
          FROM (  --Shape updated values and grab the goofy value only for grains that have them
            SELECT d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
            FROM this_key
              NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
              NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
          ) a
          --The goofy values will naturally join, inversing the grain to show the impacted leaves and this is scoped down against the granite
          NATURAL JOIN branch_15 --all dims in all roots
          NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
          NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        --Since the grain could map a leaf to more than one value, we need to make the set unique
        GROUP BY d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id
        )
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(b15) AS new_a20
        FROM branch_15
        NATURAL JOIN keystone  --all roots need to hit the keystone first
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
    ) a
    WHERE u.d2_id = a.d2_id
      AND u.d3_id = a.d3_id
      AND u.d4_id = a.d4_id
      AND u.d5_id = a.d5_id
      AND u.d8_id = a.d8_id
      AND u.d9_id = a.d9_id
      AND u.d11_id = a.d11_id
      AND u.d6_id = a.d6_id
    RETURNING u.*;
  `;

  console.log('updateApp20NatJoinSql: ', sql);
  return sql;
};

const updateApp20Sql = (params) => {
  const sql = `
    UPDATE app_20 u
    SET a20 = new_a20
    FROM (
      --elt is modified to add the keystone
      WITH keystone AS (
        WITH this_key AS (SELECT ${params.dimIdColumns} FROM ${params.tableName} WHERE active = true AND ${params.filterStatements})
        SELECT b.d2_id,b.d3_id,b.d4_id,b.d5_id,b.d8_id,b.d9_id,b.d11_id,b.d6_id
        FROM (  --Shape updated values and grab the goofy value only for grains that have them
          SELECT d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
          FROM this_key r
          JOIN grain_405 g4 ON r.d4_id = g4.d4_id
          JOIN grain_908 g9 ON r.d9_id = g9.d9_id
        ) a
        --The goofy values will naturally join, inversing the grain to show the impacted leaves and this is scoped down against the granite
        JOIN branch_15 b --all dims in all roots
          ON  b.d2_id = a.d2_id
          AND b.d3_id = a.d3_id
          AND b.d5_id = a.d5_id
          AND b.d8_id = a.d8_id
          AND b.d11_id = a.d11_id
          AND b.d6_id = a.d6_id
        JOIN grain_405 g4 ON a.goofy_d4_id = g4.goofy_d4_id AND b.d4_id = g4.d4_id
        JOIN grain_908 g9 ON a.goofy_d9_id = g9.goofy_d9_id AND b.d9_id = g9.d9_id
      --Since the grain could map a leaf to more than one value, we need to make the set unique
      GROUP BY b.d2_id,b.d3_id,b.d4_id,b.d5_id,b.d8_id,b.d9_id,b.d11_id,b.d6_id
      ) --keystone
      SELECT k.d2_id,k.d3_id,g4.goofy_d4_id AS d4_id,k.d5_id,k.d8_id,g9.goofy_d9_id AS d9_id,k.d11_id,k.d6_id,sum(b15) AS new_a20
      FROM branch_15 b
      JOIN keystone k --all roots need to hit the keystone first
        ON  b.d2_id = k.d2_id
        AND b.d3_id = k.d3_id
        AND b.d4_id = k.d4_id
        AND b.d5_id = k.d5_id
        AND b.d8_id = k.d8_id
        AND b.d9_id = k.d9_id
        AND b.d11_id = k.d11_id
        AND b.d6_id = k.d6_id
      JOIN grain_405 g4 ON k.d4_id = g4.d4_id
      JOIN grain_908 g9 ON k.d9_id = g9.d9_id
      GROUP BY k.d2_id,k.d3_id,g4.goofy_d4_id,k.d5_id,k.d8_id,g9.goofy_d9_id,k.d11_id,k.d6_id
    ) a
    WHERE u.d2_id = a.d2_id
      AND u.d3_id = a.d3_id
      AND u.d4_id = a.d4_id
      AND u.d5_id = a.d5_id
      AND u.d8_id = a.d8_id
      AND u.d9_id = a.d9_id
      AND u.d11_id = a.d11_id
      AND u.d6_id = a.d6_id;
  `;

  console.log('updateApp20Sql: ', sql);
  return sql;
};

const updateBranch15Sql = (params) => {
  const sql = `    
    UPDATE branch_15 u
    SET b15 = a.b15
    FROM ( --a
      WITH keystone AS (SELECT ${params.dimIdColumns} FROM ${params.tableName} WHERE active = true AND ${params.filterStatements})
      SELECT
        coalesce(r1.d2_id,r.d2_id) AS d2_id,
        coalesce(r1.d3_id,r.d3_id) AS d3_id,
        coalesce(r1.d4_id,r.d4_id) AS d4_id,
        coalesce(r1.d5_id,r.d5_id) AS d5_id,
        coalesce(r1.d8_id,r.d8_id) AS d8_id,
        coalesce(r1.d9_id,r.d9_id) AS d9_id,
        coalesce(r1.d11_id,r.d11_id) AS d11_id,
        coalesce(r1.d6_id,r.d6_id) AS d6_id,
        coalesce(r1,0) + coalesce(r2,0) + coalesce(r3,0) + coalesce(r4,0) - coalesce(r5,0) - coalesce(r6,0) - coalesce(r7,0) - coalesce(r8,0) + coalesce(r9,0) AS b15
      FROM (
        SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,sum(r1) AS r1
        FROM root_1 r
        JOIN keystone k ON r.d2_id = k.d2_id
          AND r.d3_id = k.d3_id
          AND r.d4_id = k.d4_id
          AND r.d5_id = k.d5_id
          AND r.d8_id = k.d8_id
          AND r.d9_id = k.d9_id
          AND r.d11_id = k.d11_id
          AND r.d6_id = k.d6_id
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
          JOIN keystone k ON r.d2_id = k.d2_id
            AND r.d3_id = k.d3_id
            AND r.d4_id = k.d4_id
            AND r.d5_id = k.d5_id
            AND r.d8_id = k.d8_id
            AND r.d9_id = k.d9_id
            AND r.d11_id = k.d11_id
            AND r.d6_id = k.d6_id
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
            JOIN keystone k ON r.d2_id = k.d2_id
              AND r.d3_id = k.d3_id
              AND r.d4_id = k.d4_id
              AND r.d5_id = k.d5_id
              AND r.d8_id = k.d8_id
              AND r.d9_id = k.d9_id
              AND r.d11_id = k.d11_id
              AND r.d6_id = k.d6_id
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
              JOIN keystone k ON r.d2_id = k.d2_id
                AND r.d3_id = k.d3_id
                AND r.d4_id = k.d4_id
                AND r.d5_id = k.d5_id
                AND r.d8_id = k.d8_id
                AND r.d9_id = k.d9_id
                AND r.d11_id = k.d11_id
                AND r.d6_id = k.d6_id
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
                JOIN keystone k ON r.d2_id = k.d2_id
                  AND r.d3_id = k.d3_id
                  AND r.d4_id = k.d4_id
                  AND r.d5_id = k.d5_id
                  AND r.d8_id = k.d8_id
                  AND r.d9_id = k.d9_id
                  AND r.d11_id = k.d11_id
                  AND r.d6_id = k.d6_id
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
                  JOIN keystone k ON r.d2_id = k.d2_id
                    AND r.d3_id = k.d3_id
                    AND r.d4_id = k.d4_id
                    AND r.d5_id = k.d5_id
                    AND r.d8_id = k.d8_id
                    AND r.d9_id = k.d9_id
                    AND r.d11_id = k.d11_id
                    AND r.d6_id = k.d6_id
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
                    JOIN keystone k ON r.d2_id = k.d2_id
                      AND r.d3_id = k.d3_id
                      AND r.d4_id = k.d4_id
                      AND r.d5_id = k.d5_id
                      AND r.d8_id = k.d8_id
                      AND r.d9_id = k.d9_id
                      AND r.d11_id = k.d11_id
                      AND r.d6_id = k.d6_id
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
                      JOIN keystone k ON r.d2_id = k.d2_id
                        AND r.d3_id = k.d3_id
                        AND r.d4_id = k.d4_id
                        AND r.d5_id = k.d5_id
                        AND r.d8_id = k.d8_id
                        AND r.d9_id = k.d9_id
                        AND r.d11_id = k.d11_id
                        AND r.d6_id = k.d6_id
                      WHERE active AND r.d3_id = 5 AND r.d8_id IN (3,4)
                      GROUP BY r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id
                      ) r8
                    FULL OUTER JOIN (
                      SELECT r.d2_id,r.d3_id,r.d4_id,r.d5_id,r.d8_id,r.d9_id,r.d11_id,r.d6_id,sum(r9) AS r9
                      FROM root_9 r
                      JOIN keystone k ON r.d2_id = k.d2_id
                        AND r.d3_id = k.d3_id
                        AND r.d4_id = k.d4_id
                        AND r.d5_id = k.d5_id
                        AND r.d8_id = k.d8_id
                        AND r.d9_id = k.d9_id
                        AND r.d11_id = k.d11_id
                        AND r.d6_id = k.d6_id
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
      ) a
    WHERE u.d2_id = a.d2_id
      AND u.d3_id = a.d3_id
      AND u.d4_id = a.d4_id
      AND u.d5_id = a.d5_id
      AND u.d8_id = a.d8_id
      AND u.d9_id = a.d9_id
      AND u.d11_id = a.d11_id
      AND u.d6_id = a.d6_id;
  `;

  console.log('updateBranch15Sql: ', sql);
  return sql;
};

module.exports = { 
  querySql,
  updateAppTableSql,
  getDimensionIdSql,
  unnestFactTableKeySql,
  deactivateSql,
  insertSql,
  sfInsertSql,
  updateBranch15NatJoinSql,
  updateApp20NatJoinSql,
  updateApp20Sql,
  updateBranch15Sql
};

