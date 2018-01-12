const debug = require('debug')('log');
const { capitalize, makeLowerCase } = require('./../util');
const { extractKeySet, extractKeySetAndId } = require('./../grid');

const makeUpdateSql = (transform, keySets) => {
  const table = transform.table;
  const metric = transform.metrics[0];
  const value = transform.new_value;
  const whereClause = keySets.map(key => {
    return `${key.dimension} = '${key.member}'`;
  });
  const queryString = `UPDATE ${table} SET "${metric}" = ${value} WHERE ${whereClause.join(
    ' AND '
  )}`;

  debug('makeUpdateQueryString: ' + queryString);

  return queryString;
};

const makeUpsertSql = (transform, dimensions) => {
  const factId = transform.factId;
  const rootTableName = `root_${factId}`;
  const dimIdColumns = dimensions.map(dim => {
    return dim.idColName;
  });
  const filterStatements = dimensions.map(dim => {
    return dim.idWhereClause;
  }).join(' AND ');
  
  // 1. Inactivate the old value and insert the new value
  const upsertSql = `
    WITH
      inactive AS (
        UPDATE ${rootTableName} u
        SET active = false
        WHERE active = true AND ${filterStatements}
        RETURNING ${dimIdColumns}
        )
    INSERT INTO ${rootTableName} (${dimIdColumns}, r${factId})
    SELECT *,${transform.new_value}::DOUBLE PRECISION FROM inactive;
  `;

  // 2. Propogate through matrix (branch_15 and app_20)
  const propMatrixSql = `
    UPDATE branch_15 u
    SET b15 = new_b15
    FROM (
      WITH keystone AS (SELECT ${dimIdColumns} FROM ${rootTableName} WHERE active = true AND ${filterStatements})
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
    
    UPDATE app_20 u
    SET a20 = new_a20
    FROM (
        --elt is modified to add the keystone
        WITH keystone AS (
          WITH this_key AS (SELECT ${dimIdColumns} FROM ${rootTableName} WHERE active = true AND ${filterStatements})
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
  
  return upsertSql;
};

const makePropMatrixSqlPg = (transform, dimensions) => {
  const factId = transform.factId;
  const rootTableName = `root_${factId}`;
  const dimIdColumns = dimensions.map(dim => {
    return dim.idColName;
  });
  const filterStatements = dimensions.map(dim => {
    return dim.idWhereClause;
  }).join(' AND ');

  const sql = `
    UPDATE branch_15 u
    SET b15 = new_b15
    FROM (
      WITH keystone AS (SELECT ${dimIdColumns} FROM ${rootTableName} WHERE active = true AND ${filterStatements})
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
    
    UPDATE app_20 u
    SET a20 = new_a20
    FROM (
        --elt is modified to add the keystone
        WITH keystone AS (
          WITH this_key AS (SELECT ${dimIdColumns} FROM ${rootTableName} WHERE active = true AND ${filterStatements})
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

  return sql;
};

// TODO
const makePropMatrixSqlSf = (transform, dimensions) => {
  const factId = transform.factId;
  const rootTableName = `root_${factId}`;
  const dimIdColumns = dimensions.map(dim => {
    return dim.idColName;
  });
  const filterStatements = dimensions.map(dim => {
    return dim.idWhereClause;
  }).join(' AND ');

  const sql = `TODO`;

  return sql;
};

const makeQuerySql = (transform, pinned, dimKeys, metrics) => {
  const table = transform.table;
  const dimensions = transform.dimensions.join(',');
  const filterStatements = buildFilterStatement(pinned, transform.isNile, dimKeys);
  const queryString = `SELECT ${dimensions},${metrics} FROM ${
    table
  } WHERE ${filterStatements.join(' AND ')};`;

  // debug('makeQueryString: ' + queryString);
  // debug("pinnedSet: " + JSON.stringify(pinnedSet));
  // debug("table: " + table);
  // debug("dimensions: " + dimensions);
  // debug("metrics: " + metrics);
  // debug("filterStatements: " + filterStatements);
  // debug('queryString: ', queryString);

  return queryString;
};

const makeUnnestFactTableKeysSql = factId => {
  // Exract fact keys
  const factKeySql = `
    SELECT *
    FROM (SELECT unnest(fact_key) AS dim_id FROM s_fact WHERE fact_id = ${fact.fact_id}) a
    NATURAL JOIN s_dim ORDER BY dim_byte, dim_id;`;
};

const buildFilterStatement = (filters, isNile, dimKeys) => {
  // TODO: replace this with an alternative way to distinguish Nile vs v1 manifests.
  if (isNile) {
    return filters.map(filter => {
      // Get dimension info
      const dimInfo = dimKeys.find(dimKey => {
        return dimKey.name === filter.dimension;
      });

      return `d${dimInfo.id}_id = (
        SELECT d${dimInfo.id}_id
        FROM dim_${dimInfo.id}
        WHERE d${dimInfo.id}_name = '${filter.member}'
      )`;
    });
  }
  else {
    return filters.map(filter => {
      return `${capitalize(filter.dimension)} IN ('${filter.member}')`;
    });
  }
};

const makeDimSql = dimensions => {
  const subSelects = dimensions.map(dim => {
    return `(SELECT d${dim.id}_id
      FROM dim_${dim.id}
      WHERE d${dim.id}_name = '${dim.member}')`;
  });

  const dimIdSql = `
    SELECT row_to_json(columns) AS results
    FROM (SELECT ${subSelects}) AS columns;
  `;
  return dimIdSql;
}

module.exports = { 
  makeQuerySql, 
  makeUpdateSql,
  makeUpsertSql,
  makeUnnestFactTableKeysSql,
  makeDimSql,
  makePropMatrixSqlPg,
  makePropMatrixSqlSf
};
