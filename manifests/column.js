const { flatten, completelyFlatten } = require('./util');

const buildColumnDef = columns => {
  const colDef = {
    columns: [],
    properties: {}
  };

  const field = columns.map(column => column.member).join('_');
  const editable = columns[columns.length - 1]['data entry']; // Get the last element in the array

  columns
    .reverse()
    .forEach((column, i) =>
      colDef.columns.push({ value: column.member, level: i })
    );
  colDef.properties = Object.assign({}, { field, editable });

  return colDef;
};

const buildColumnDefs = columns => {
  return columns.map(c => buildColumnDef(c));
};

const transformColumnDefs = columns => {
  const transform = (column, parents) => {
    parents = [...parents, column];
    return column.columns
      ? column.columns.map(c => transform(c, parents))
      : parents;
  };
  return columns.map(column => transform(column, []));
};

const buildDynamicColumns = (columns, depth) => {
  const transformedColumns = transformColumnDefs(columns);
  const flattenedColumns = flatten(transformedColumns, depth - 1);
  const builtColumns = buildColumnDefs(flattenedColumns);

  return builtColumns;
};

const buildColumnsForRegion = (region, colDepth) => {
  const transformedColumns = buildDynamicColumns(region.columns, colDepth);
  return completelyFlatten(transformedColumns);
};

const getInnermostColumns = columns => {
  const drill = cols => (cols[0].columns ? drill(cols[0].columns) : cols);

  return drill(columns);
};

const addVariationColumn = columns => {
  const column = {
    dimension: 'Scenario',
    member: 'Variance',
    'data entry': false
  };

  return columns.push(column);
};

const addVariationPercent = columns => {
  const column = {
    dimension: 'Scenario',
    member: 'Variance %',
    'data entry': false
  };

  return columns.push(column);
};

const addVariationColumns = (region, columns) => {
  if (region.includeVariance) {
    addVariationColumn(columns);
  }
  if (region.includeVariancePct) {
    addVariationPercent(columns);
  }
  return columns;
};

module.exports = {
  buildColumnsForRegion,
  getInnermostColumns,
  addVariationColumns
};
