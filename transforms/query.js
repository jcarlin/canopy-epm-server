const debug = require('debug')('log');
const { capitalize, makeLowerCase } = require('./../util');
const { extractKeySet, extractKeySetAndId } = require('./../grid');

const getPinnedSet = pinned => {
  return pinned.filter(pin => pin.dimension !== 'Metric').map(pin => {
    return {
      dimension: makeLowerCase(pin.dimension),
      member: pin.member
    };
  });
};

const buildFilterStatement = (filters, tableName, dimKeys) => {
  // TODO: replace this with an alternative way to distinguish Nile vs v1 manifests.
  if (tableName.match('elt.')) {
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



const cxUpsertQueryString = (transform, ice, pinned, dimKeys) => {
  const keySets = [
    ...extractKeySetAndId(ice.rowKey),
    ...extractKeySetAndId(ice.columnKey),
    ...getPinnedSet(pinned)
  ];

  const dimArrays = keySets.map(filter => {
    // Get dimension info
    const dimInfo = dimKeys.find(dimKey => {
      return dimKey.name === filter.dimension;
    });

    return `['${filter.dimension}', '${filter.member}']`;
  });

  // TODO: add handling multiple metrics (array) from manifest
  const queryString = `
    SELECT cx_upsert(
      ARRAY[
      ['root_name','${transform.metrics[0]}'],
      ${dimArrays},
      ['value','${transform.new_value}'],
      ['skip_execute','off'],
      ['p_msg','on']
      ]::hstore
    ); 
  `;

  return queryString;
};

const makeQueryString = (transform, pinned, dimKeys) => {
  const pinnedSet = getPinnedSet(pinned);
  const table = transform.table;
  const dimensions = transform.dimensions.join(',');
  const metrics = transform.metrics.map(metric => `'${metric}'`).join(',');
  const filterStatements = buildFilterStatement(pinnedSet, transform.table, dimKeys);
  const queryString = `SELECT ${dimensions},${metrics} FROM ${
    table
  } WHERE ${filterStatements.join(' AND ')};`;

  // debug('makeQueryString: ' + queryString);
  // debug("pinnedSet: " + JSON.stringify(pinnedSet));
  // debug("table: " + table);
  // debug("dimensions: " + dimensions);
  // debug("metrics: " + metrics);
  // debug("filterStatements: " + filterStatements); */
  // debug('queryString: ', queryString);

  return queryString;
};

module.exports = { 
  makeQueryString, 
  makeUpdateQueryString,
  cxUpsertQueryString
};
