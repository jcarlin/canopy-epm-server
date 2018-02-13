const debug = require('debug')('log');

const makeGrainBrickQueryStrings = (params) => {
  const members = params.members;
  const grainTableName = params.grainTableName;
  const grainSerName = params.grainSerName;
  const dimNumber = params.dimNumber;
  const dimIdName = `d${dimNumber}_id`;
  const dimByte = params.dimByte;
  const dimTableName = `dim_${dimNumber}`;
  const grainViewName = `grain_${params.grainDefName}`;
  
  const grainTableSql =
    ` DROP TABLE IF EXISTS ${grainTableName} CASCADE;
      CREATE TABLE IF NOT EXISTS ${grainTableName} (
        epoch_id     SMALLINT DEFAULT 1,
        ${grainSerName}  SERIAL,
        ${dimIdName} ${dimByte} NOT NULL);`;

  const addPrimaryKeySql = 
    ` ALTER TABLE ${grainTableName} ADD PRIMARY KEY (${dimIdName});
      CREATE UNIQUE INDEX id${grainTableName} ON ${grainTableName} (${grainSerName});
      ANALYZE ${grainTableName};`;

  const grainTableInsertSql =
    ` INSERT INTO ${grainTableName} (${dimIdName})
      SELECT ${dimIdName}
      FROM ${dimTableName}
        NATURAL JOIN (SELECT (row_number() over())::smallint AS oid, unnest AS d${dimNumber}_name FROM (SELECT unnest(string_to_array(${members}, ','))) a) a
      ORDER BY oid;`;

  const grainTableSelectSql = 
    ` SELECT * FROM ${grainTableName};`;

  const grainViewSql = 
    ` CREATE OR REPLACE VIEW ${grainViewName} AS
      SELECT ${grainSerName}, b.${dimIdName}, r.d${dimNumber}_name as scenario_id
      FROM ${grainTableName} b
        JOIN ${dimTableName} r on r.${dimIdName} = b.${dimIdName}
      ORDER BY ${grainSerName};`;

  // return them all as one string with some line breaks in between for debuging
  return [ `${grainTableSql}

            ${grainTableInsertSql}

            ${addPrimaryKeySql}

            ${grainViewSql}
            
            ` ];
};

const makeGrainBlockQueryStrings = (params, options) => {
  const members = params.members;
  const grainTableName = params.grainTableName;
  const grainSerName = params.grainSerName;
  const dimNumber = params.dimNumber;
  const dimIdName = `d${dimNumber}_id`;
  const hierNumber = params.hierNumber;
  const hierIdName = `d${hierNumber}_id`;
  const goofyDimIdName = `goofy_${dimIdName}`;
  const dimByte = params.dimByte;
  const dimTableName = `dim_${dimNumber}`;
  const grainViewName = `grain_${params.grainDefName}`;
  const hierName = params.hierName;
  const grainDefId = params.grainDefId;
  
  const grainTableSql =
    ` DROP TABLE IF EXISTS ${grainTableName} CASCADE;
      CREATE TABLE IF NOT EXISTS ${grainTableName} (
        epoch_id     SMALLINT DEFAULT 1,
        ${grainSerName}  SERIAL,
        ${dimIdName} ${dimByte} NOT NULL,
        ${goofyDimIdName} ${dimByte} NOT NULL);`;

  const addPrimaryKeySql = 
    ` ALTER TABLE ${grainTableName} ADD PRIMARY KEY (${dimIdName}, ${goofyDimIdName});
      CREATE UNIQUE INDEX id${grainTableName} ON ${grainTableName} (${grainSerName});
      ANALYZE ${grainTableName};`;

  let grainTableInsertSql = "";
  let grainViewSql = "";

  if (options.memberSet.platformType === "parent_child") {
    grainTableInsertSql =
    ` INSERT INTO ${grainTableName} (${dimIdName}, ${goofyDimIdName})
      WITH RECURSIVE tree AS (
        SELECT
          0 AS depth, 
          0 AS parent_ord, 
          d${dimNumber}h${hierNumber}_oid AS child_ord,
          d${dimNumber}h${hierNumber}_parent_id AS parent_id,
          ${dimIdName} AS child_id
        FROM ${dimTableName}
        WHERE d${dimNumber}h${hierNumber}_parent_id = 1 
        UNION
        SELECT 
          t.depth+1 AS depth, 
          t.child_ord AS parent_ord, 
          d${dimNumber}h${hierNumber}_oid AS child_ord, 
          d${dimNumber}h${hierNumber}_parent_id AS parent_id, 
          ${dimIdName} AS child_id 
        FROM ${dimTableName} d 
          JOIN tree t ON d.d${dimNumber}h${hierNumber}_parent_id = t.child_id
      )
      SELECT parent_id, child_id FROM tree ORDER BY depth, parent_ord, child_ord;`;

    grainViewSql = 
      ` CREATE OR REPLACE VIEW ${grainViewName} AS
        SELECT epoch_name, ${grainSerName}, b.${dimIdName}, l.d${dimNumber}_name as ${hierName}_parent, b.${goofyDimIdName}, r.d${dimNumber}_name as ${hierName}_child
        FROM ${grainTableName} b
          NATURAL JOIN s_epoch e
          JOIN dim_${dimNumber} l on l.${dimIdName} = b.${dimIdName}
          JOIN dim_${dimNumber} r on r.${dimIdName} = b.${goofyDimIdName}
        ORDER BY ${grainSerName};`;
  } else if (options.memberSet.platformType === "node_leaf") {
    grainTableInsertSql =
    ` INSERT INTO ${grainTableName} (${dimIdName}, ${goofyDimIdName})
      WITH RECURSIVE tree AS (
        SELECT 
          0 AS depth, 
          0 AS node_ord, 
          d${dimNumber}h${hierNumber}_oid AS leaf_ord, 
          d${dimNumber}h${hierNumber}_parent_id AS node_id,
          ${dimIdName} AS leaf_id 
        FROM dim_${dimNumber} 
          NATURAL JOIN (
            SELECT * FROM (
              SELECT ${dimIdName} 
              FROM ${dimTableName} 
              WHERE d${dimNumber}h${hierNumber}_parent_id IS NOT NULL) d 
              
              EXCEPT (
                SELECT DISTINCT d${dimNumber}h${hierNumber}_parent_id AS ${dimIdName}
                FROM ${dimTableName} 
                WHERE d${dimNumber}h${hierNumber}_parent_id IS NOT NULL)
            ) l 
        UNION ALL
        SELECT
          t.depth+1 AS depth, 
          d.d${dimNumber}h${hierNumber}_oid AS node_ord, 
          t.leaf_ord AS leaf_ord, 
          d.${dimIdName} AS node_id, 
          t.leaf_id AS leaf_id 
        FROM ${dimTableName} d
          JOIN (
            SELECT depth, leaf_ord, ${dimIdName} AS node_id, leaf_id 
            FROM tree t 
              JOIN ${options.memberSet.parentTableName} ON t.node_id = ${goofyDimIdName}
          ) t ON d.${dimIdName} = t.node_id )
      SELECT node_id, leaf_id FROM tree ORDER BY depth DESC, node_ord, leaf_ord;`;

    grainViewSql = 
      ` CREATE OR REPLACE VIEW ${grainViewName} AS
        SELECT epoch_name, ${grainSerName}, b.${dimIdName}, l.d${dimNumber}_name as ${hierName}_node, b.${goofyDimIdName}, r.d${dimNumber}_name as ${hierName}_leaf
        FROM ${grainTableName} b
          NATURAL JOIN s_epoch e
          JOIN dim_${dimNumber} l on l.${dimIdName} = b.${dimIdName}
          JOIN dim_${dimNumber} r on r.${dimIdName} = b.${goofyDimIdName}
        ORDER BY ${grainSerName};`;
  } else if (options.memberSet.platformType === "subtree") {
    grainTableInsertSql =
    ` INSERT INTO ${grainTableName} (${dimIdName}, ${goofyDimIdName})
      SELECT t.* FROM (
        WITH RECURSIVE tree AS (
          SELECT d.d${dimNumber}h${hierNumber}_parent_id AS node_id, d.${dimIdName} AS child_id 
          FROM ${dimTableName} d 
          WHERE d.d${dimNumber}h${hierNumber}_parent_id = 1
			    UNION
			    SELECT * FROM (   				    
            WITH this_tree AS (SELECT node_id, child_id FROM tree)
            SELECT d.d${dimNumber}h${hierNumber}_parent_id AS node_id, d.${dimIdName} AS child_id
            FROM ${dimTableName} d
				      JOIN this_tree t ON t.child_id = d.d${dimNumber}h${hierNumber}_parent_id
				    UNION ALL  
            SELECT t.node_id AS node_id, d.${dimIdName} AS child_id
            FROM ${dimTableName} d
				      JOIN this_tree t ON t.child_id = d.d${dimNumber}h${hierNumber}_parent_id
          ) a 
        )
		    SELECT node_id AS ${dimIdName}, child_id AS ${goofyDimIdName} FROM tree
		  ) t
	      JOIN ${dimTableName} l on l.${dimIdName} = t.${dimIdName}
	      JOIN ${dimTableName} r on r.${dimIdName} = t.${goofyDimIdName}
	    ORDER BY l.d${dimNumber}h${hierNumber}_oid, r.d${dimNumber}h${hierNumber}_oid;`;

    grainViewSql = 
      ` CREATE OR REPLACE VIEW ${grainViewName} AS
        SELECT epoch_name, ${grainSerName}, b.${dimIdName}, l.d${dimNumber}_name as ${hierName}_root, b.${goofyDimIdName}, r.d${dimNumber}_name as ${hierName}_subtree
        FROM ${grainTableName} b
          NATURAL JOIN s_epoch e
          JOIN dim_${dimNumber} l on l.${dimIdName} = b.${dimIdName}
          JOIN dim_${dimNumber} r on r.${dimIdName} = b.${goofyDimIdName}
        ORDER BY ${grainSerName};`;
  } else if (options.memberSet.memberSetType === "compute") {
    grainTableInsertSql =
    ` INSERT INTO ${grainTableName} (${dimIdName}, ${goofyDimIdName})
      ${options.memberSet.memberSetCode.code};`;

    grainViewSql = 
      ` CREATE OR REPLACE VIEW ${grainViewName} AS
        SELECT epoch_name, ${grainSerName}, b.${dimIdName}, l.d${dimNumber}_name as ${options.memberSet.dimColumnName}, b.${goofyDimIdName}, r.d${dimNumber}_name as ${options.memberSet.goofyColumnName}
        FROM ${grainTableName} b
          NATURAL JOIN s_epoch e
          JOIN dim_${dimNumber} l on l.${dimIdName} = b.${dimIdName}
          JOIN dim_${dimNumber} r on r.${dimIdName} = b.${goofyDimIdName}
        ORDER BY ${grainSerName};`;
  }

  const grainTableSelectSql = 
    ` SELECT * FROM ${grainTableName};`;

  // return them all as one string with some line breaks in between for debuging
  return [ `${grainTableSql}

            ${grainTableInsertSql}

            ${addPrimaryKeySql}

            ${grainViewSql}
            
            ` ];
};

const makeObjectCodeByTimeView = () => {
  return `
    CREATE OR REPLACE VIEW object_code_by_time AS 
    SELECT dim_2.d2_id,
        dim_2.d2_name AS product_id,
        dim_3.d3_id,
        dim_3.d3_name AS currency_id,
        dim_4.d4_id,
        dim_4.d4_name AS datasrc_id,
        dim_5.d5_id,
        dim_5.d5_name AS objectcode_id,
        dim_8.d8_id,
        dim_8.d8_name AS scenario_id,
        dim_9.d9_id,
        dim_9.d9_name AS time_id,
        dim_11.d11_id,
        dim_11.d11_name AS variation_id,
        a.d6_id,
        dim_6.d6_name AS organization_id,
        a.r6 AS expense_balance
      FROM ( SELECT root_6.d2_id,
                root_6.d3_id,
                root_6.d4_id,
                root_6.d5_id,
                root_6.d8_id,
                root_6.d9_id,
                root_6.d11_id,
                root_6.d6_id,
                root_6.r6
              FROM root_6
                JOIN grain_202 USING (d2_id)
                JOIN grain_301 USING (d3_id, epoch_id)
                JOIN grain_406 USING (epoch_id, d4_id)
                JOIN grain_503 USING (epoch_id, d5_id)
                JOIN grain_801 USING (epoch_id, d8_id)
                JOIN grain_903 USING (epoch_id, d9_id)
                JOIN grain_1102 USING (epoch_id, d11_id)
                JOIN grain_602 USING (epoch_id, d6_id)
              WHERE root_6.active) a
        JOIN dim_2 USING (d2_id)
        JOIN dim_3 USING (d3_id)
        JOIN dim_4 USING (d4_id)
        JOIN dim_5 USING (d5_id)
        JOIN dim_8 USING (d8_id)
        JOIN dim_9 USING (d9_id)
        JOIN dim_11 USING (d11_id)
        JOIN dim_6 USING (d6_id);
    
    ALTER TABLE object_code_by_time
      OWNER TO canopy_db_admin;
  `;
};

const makeAppNetRevView = () => {
  return `
    CREATE OR REPLACE VIEW app_net_rev AS 
    SELECT dim_2.d2_id,
        dim_2.d2_name AS product_id,
        dim_3.d3_id,
        dim_3.d3_name AS currency_id,
        dim_4.d4_id,
        dim_4.d4_name AS datasrc_id,
        dim_5.d5_id,
        dim_5.d5_name AS objectcode_id,
        dim_5.d5_desc AS objectcode_desc,
        dim_8.d8_id,
        dim_8.d8_name AS scenario_id,
        dim_9.d9_id,
        dim_9.d9_name AS time_id,
        dim_11.d11_id,
        dim_11.d11_name AS variation_id,
        a.d6_id,
        dim_6.d6_name AS organization_id,
        a.a20 AS app_net_rev
      FROM ( SELECT grain_206.goofy_d2_id AS d2_id,
                app_20.d3_id,
                app_20.d4_id,
                grain_515.goofy_d5_id AS d5_id,
                app_20.d8_id,
                app_20.d9_id,
                app_20.d11_id,
                grain_606.goofy_d6_id AS d6_id,
                sum(app_20.a20) AS a20
              FROM app_20
                JOIN grain_206 USING (epoch_id, d2_id)
                JOIN grain_515 USING (epoch_id, d5_id)
                JOIN grain_606 USING (epoch_id, d6_id)
              GROUP BY grain_206.goofy_d2_id, app_20.d3_id, app_20.d4_id, grain_515.goofy_d5_id, app_20.d8_id, app_20.d9_id, app_20.d11_id, grain_606.goofy_d6_id) a
        JOIN dim_2 USING (d2_id)
        JOIN dim_3 USING (d3_id)
        JOIN dim_4 USING (d4_id)
        JOIN dim_5 USING (d5_id)
        JOIN dim_8 USING (d8_id)
        JOIN dim_9 USING (d9_id)
        JOIN dim_11 USING (d11_id)
        JOIN dim_6 USING (d6_id)
      ORDER BY dim_5.d5_desc;
    
    ALTER TABLE app_net_rev
      OWNER TO canopy_db_admin;  
  `;
}

module.exports = { 
  makeGrainBrickQueryStrings,
  makeGrainBlockQueryStrings,
  makeObjectCodeByTimeView,
  makeAppNetRevView
};
