const { capitalize } = require('./../util');
const { extractKeySet } = require('./../manifests');

const buildFilterStatement = filters => {
  return filters.map(filter => {
    return `${capitalize(filter.dim)} IN (${filter.values
      .map(val => `'${val}'`)
      .join(',')})`;
  });
};

const makeUpdateQuery = (transform, ice) => {
  const keys = Object.keys(ice);
  const keySets = [
    ...extractKeySet(ice.rowKey),
    ...extractKeySet(ice.columnKey)
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

const makeQuery = transform => {
  const table = transform.table;
  const dimensions = transform.dimensions.join(',');
  const metrics = transform.metrics.map(metric => `"${metric}"`).join(',');
  const filterStatements = buildFilterStatement(transform.filters);
  const query = `SELECT ${dimensions},${metrics} FROM ${
    table
  } WHERE ${filterStatements.join(' AND ')}`;
  return query;
};

module.exports = { makeQuery, makeUpdateQuery };
