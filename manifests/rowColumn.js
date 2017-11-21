const { flatten } = require('./util');

const getFirstRows = (region, rowDepth) => {
  const traverse = ({ rows }, depth, parents) => {
    if (depth > 0) {
      return traverse(rows[0], depth - 1, [...parents, rows[0]]);
    } else {
      return parents;
    }
  };

  return traverse(region, rowDepth, []);
};

const buildRowColumn = (depth, field) => {
  const colDef = {
    columns: [],
    properties: {}
  };

  for (let i = 0; i < depth; ++i) {
    colDef.columns.push({ value: '', level: i });
  }
  colDef.properties = Object.assign(
    {},
    { field: field['dimension'], editable: field['data entry'] }
  );

  return colDef;
};

const buildRowColumns = (region, colDepth, rowDepth) => {
  return getFirstRows(region, rowDepth).map(row =>
    buildRowColumn(colDepth, row)
  );
};

module.exports = { buildRowColumns };
