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

const buildFilterStatement = filters => {
  return filters.map(filter => {
    return `${capitalize(filter.dimension)} IN ('${filter.member}')`;
  });
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
  const queryString = keySets.map(key => {
    return `${key.dimension} = '${key.member}'`;
  });

  return `UPDATE ${table} SET "${metric}" = ${value} WHERE ${queryString.join(
    ' AND '
  )}`;
};

const makeQueryString = (transform, pinned) => {
  const pinnedSet = getPinnedSet(pinned);
  const table = transform.table;
  const dimensions = transform.dimensions.join(',');
  const metrics = transform.metrics.map(metric => `"${metric}"`).join(',');
  const filterStatements = buildFilterStatement(pinnedSet);
  const queryString = `SELECT ${dimensions},${metrics} FROM ${
    table
  } WHERE ${filterStatements.join(' AND ')}`;
  return queryString;
};

module.exports = { makeQueryString, makeUpdateQueryString };
