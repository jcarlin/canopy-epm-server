const { capitalize } = require('./../util');

const makeQuery = rawTransform => {
  const transform = JSON.parse(rawTransform);
  const table = transform.table;
  const dimensions = transform.dimensions.join(',');
  const metrics = transform.metrics.map(metric => `"${metric}"`).join(',');
  const filterStatements = transform.filters.map(filter => {
    return `${capitalize(filter.dim)} IN (${filter.values
      .map(val => `'${val}'`)
      .join(',')})`;
  });
  const query = `SELECT ${dimensions},${metrics} FROM ${table} WHERE ${filterStatements.join(
    ' AND '
  )}`;
  return query;
};

module.exports = makeQuery;
