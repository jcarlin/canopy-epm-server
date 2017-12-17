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
  return queryString;
};

module.exports = { makeQueryString, makeUpdateQueryString };
