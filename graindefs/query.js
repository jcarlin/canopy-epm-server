const upsertGraindefSql = (params) => {
  const sql = `
    INSERT INTO model.graindef (graindef_id, graindef_name, graindef_dimensions, graindef_graindef)
    VALUES (${params.id}, '${params.name}', '{${params.dimension}}', '${params.graindefString}'::json)
    ON CONFLICT (graindef_id, graindef_name) DO 
    UPDATE SET
      graindef_id = EXCLUDED.graindef_id,
      graindef_dimensions = EXCLUDED.graindef_dimensions,
      graindef_graindef = EXCLUDED.graindef_graindef
    WHERE graindef.graindef_name = EXCLUDED.graindef_name;`;

  return sql;
};

const makeGrainBrickSql = (params) => {
  const members = params.members;
  const grainTableName = params.grainTableName;
  const grainSerName = params.grainSerName;
  const dimNumber = params.dimNumber;
  const dimIdName = `d${dimNumber}_id`;
  const dimByte = params.dimByte;
  const dimTableName = `dim_${dimNumber}`;
  const grainViewName = `grain_${params.graindefName}`;
  const schema = params.schema;
  
  const grainTableSql =
    ` DROP TABLE IF EXISTS ${schema}.${grainTableName} CASCADE;
      CREATE TABLE IF NOT EXISTS ${schema}.${grainTableName} (
        epoch_id     SMALLINT DEFAULT 1,
        ${grainSerName}  SERIAL,
        ${dimIdName} ${dimByte} NOT NULL);`;

  const addPrimaryKeySql = 
    ` ALTER TABLE ${schema}.${grainTableName} ADD PRIMARY KEY (${dimIdName});
      CREATE UNIQUE INDEX IF NOT EXISTS id${grainTableName} ON ${grainTableName} (${grainSerName});
      ANALYZE ${grainTableName};`;

  const grainTableInsertSql =
    ` INSERT INTO ${schema}.${grainTableName} (${dimIdName})
      SELECT ${dimIdName}
      FROM ${dimTableName}
        NATURAL JOIN (SELECT (row_number() over())::smallint AS oid, unnest AS d${dimNumber}_name FROM (SELECT unnest(string_to_array(${members}, ','))) a) a
      ORDER BY oid;`;

  const grainTableSelectSql = 
    ` SELECT * FROM ${schema}.${grainTableName};`;

  const grainViewSql = 
    ` CREATE OR REPLACE VIEW ${schema}.${grainViewName} AS
      SELECT ${grainSerName}, b.${dimIdName}, r.d${dimNumber}_name as scenario_id
      FROM ${schema}.${grainTableName} b
        JOIN ${dimTableName} r on r.${dimIdName} = b.${dimIdName}
      ORDER BY ${grainSerName};`;

  // return them all as one string with some line breaks in between for debuging
  return [ `${grainTableSql}

            ${grainTableInsertSql}

            ${addPrimaryKeySql}

            ${grainViewSql}
            
            ` ];
};

const makeGrainBlockSql = (params, options) => {
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
  const grainViewName = `grain_${params.graindefName}`;
  const hierName = params.hierName;
  const graindefId = params.graindefId;
  const schema = params.schema;
  
  const grainTableSql =
    ` DROP TABLE IF EXISTS ${schema}.${grainTableName} CASCADE;
      CREATE TABLE IF NOT EXISTS ${schema}.${grainTableName} (
        epoch_id     SMALLINT DEFAULT 1,
        ${grainSerName}  SERIAL,
        ${dimIdName} ${dimByte} NOT NULL,
        ${goofyDimIdName} ${dimByte} NOT NULL);`;

  const addPrimaryKeySql = 
    ` ALTER TABLE ${schema}.${grainTableName} ADD PRIMARY KEY (${dimIdName}, ${goofyDimIdName});
      CREATE UNIQUE INDEX IF NOT EXISTS id${grainTableName} ON ${grainTableName} (${grainSerName});
      ANALYZE ${grainTableName};`;

  let grainTableInsertSql = "";
  let grainViewSql = "";

  if (options.memberSet.platformType === "parent_child") {
    grainTableInsertSql =
    ` INSERT INTO ${schema}.${grainTableName} (${dimIdName}, ${goofyDimIdName})
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
      ` CREATE OR REPLACE VIEW ${schema}.${grainViewName} AS
        SELECT epoch_name, ${grainSerName}, b.${dimIdName}, l.d${dimNumber}_name as ${hierName}_parent, b.${goofyDimIdName}, r.d${dimNumber}_name as ${hierName}_child
        FROM ${grainTableName} b
          NATURAL JOIN s_epoch e
          JOIN dim_${dimNumber} l on l.${dimIdName} = b.${dimIdName}
          JOIN dim_${dimNumber} r on r.${dimIdName} = b.${goofyDimIdName}
        ORDER BY ${grainSerName};`;
  } else if (options.memberSet.platformType === "node_leaf") {
    grainTableInsertSql =
    ` INSERT INTO ${schema}.${grainTableName} (${dimIdName}, ${goofyDimIdName})
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
      ` CREATE OR REPLACE VIEW ${schema}.${grainViewName} AS
        SELECT epoch_name, ${grainSerName}, b.${dimIdName}, l.d${dimNumber}_name as ${hierName}_node, b.${goofyDimIdName}, r.d${dimNumber}_name as ${hierName}_leaf
        FROM ${grainTableName} b
          NATURAL JOIN s_epoch e
          JOIN dim_${dimNumber} l on l.${dimIdName} = b.${dimIdName}
          JOIN dim_${dimNumber} r on r.${dimIdName} = b.${goofyDimIdName}
        ORDER BY ${grainSerName};`;
  } else if (options.memberSet.platformType === "subtree") {
    grainTableInsertSql =
    ` INSERT INTO ${schema}.${grainTableName} (${dimIdName}, ${goofyDimIdName})
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
      ` CREATE OR REPLACE VIEW ${schema}.${grainViewName} AS
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
      ` CREATE OR REPLACE VIEW ${schema}.${grainViewName} AS
        SELECT epoch_name, ${grainSerName}, b.${dimIdName}, l.d${dimNumber}_name as ${options.memberSet.dimColumnName}, b.${goofyDimIdName}, r.d${dimNumber}_name as ${options.memberSet.goofyColumnName}
        FROM ${grainTableName} b
          NATURAL JOIN s_epoch e
          JOIN dim_${dimNumber} l on l.${dimIdName} = b.${dimIdName}
          JOIN dim_${dimNumber} r on r.${dimIdName} = b.${goofyDimIdName}
        ORDER BY ${grainSerName};`;
  }

  const grainTableSelectSql = 
    ` SELECT * FROM ${schema}.${grainTableName};`;

  // return them all as one string with some line breaks in between for debuging
  return [ `${grainTableSql}
            ${grainTableInsertSql}
            ${addPrimaryKeySql}
            ${grainViewSql}
            
            ` ];
};


module.exports = { 
  upsertGraindefSql,
  makeGrainBrickSql,
  makeGrainBlockSql
};
