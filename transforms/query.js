const debug = require('debug')('log');
const { capitalize, makeLowerCase } = require('./../util');
const { extractKeySet } = require('./../grid');

const getPinnedSet = pinned => {
  return pinned.filter(pin => pin.dimension !== 'Metric').map(pin => {
    return {
      dimension: makeLowerCase(pin.dimension),
      member: pin.member
    };
  });
};

const buildFilterStatement = (filters, tableName) => {
  // TODO: replace this hack that was created to rectify Nile vs public and manifest naming conventions.
  if (tableName === "elt.app_net_rev") {
    return filters.map(filter => {
      return `${makeLowerCase(filter.dimension)}_id = ('${filter.member}')`;
    });  
  }
  else {
    return filters.map(filter => {
      return `${capitalize(filter.dimension)} IN ('${filter.member}')`;
    });
  }
};

const makeUpdateQueryString = (transform, ice, pinned) => {
  const keys = Object.keys(ice);

  const pinnedSet = getPinnedSet(pinned);

  const keySets = [
    ...extractKeySet(ice.rowKey),
    ...extractKeySet(ice.columnKey),
    ...pinnedSet
  ];
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

const makeQueryString = (transform, pinned) => {
  const pinnedSet = getPinnedSet(pinned);
  const table = transform.table;
  const dimensions = transform.dimensions.join(',');
  const metrics = transform.metrics.map(metric => `"${metric}"`).join(',');
  const filterStatements = buildFilterStatement(pinnedSet, transform.table);
  const queryString = `SELECT ${dimensions},${metrics} FROM ${
    table
  } WHERE ${filterStatements.join(' AND ')}`;

  debug('makeQueryString: ' + queryString);
  debug("pinnedSet: " + JSON.stringify(pinnedSet));
  debug("table: " + table);
  debug("dimensions: " + dimensions);
  debug("metrics: " + metrics);
  debug("filterStatements: " + filterStatements);
  return queryString;
};

const makeGrainDimQueryString = (dimension) => {
  const dimSql = `
    SELECT dim_id 
      FROM s_dim 
     WHERE dim_name = '${dimension}';`;

  return `${dimSql}`;
};

const makeGrainQueryStrings = (params) => {
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

            ${addPrimaryKeySql}

            ${grainTableInsertSql}

            ${grainTableSelectSql}

            ${grainViewSql}
            
            ` ];
};

module.exports = { 
  makeQueryString, 
  makeUpdateQueryString,
  makeGrainDimQueryString, 
  makeGrainQueryStrings
};
