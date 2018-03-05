const { flatten, completelyFlatten } = require('./util');

const buildColumnDef = (columns, region) => {
  const colDef = {
    columns: [],
    properties: {}
  };

  const field = columns.map(column => column.member).join('__');
  const editable = columns[columns.length - 1]['data entry']; // Get the last element in the array
  const rowIndex = region.rowIndex;
  const colIndex = region.colIndex;

  columns
    .reverse()
    .forEach((column, i) =>
      colDef.columns.push({
        dimension: column.dimension,
        description: column.description,
        value: column.description || column.member, // in case description is not a key present in the manifest, use member
        member: column.member,
        level: i
      })
    );
  colDef.properties = Object.assign({}, { field, editable, rowIndex, colIndex });

  return colDef;
};

const buildColumnDefs = (columns, region) => {
  return columns.map(c => buildColumnDef(c, region));
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

const buildDynamicColumns = (region, depth) => {
  const transformedColumns = transformColumnDefs(region.columns);
  const flattenedColumns = flatten(transformedColumns, depth - 1);
  const builtColumns = buildColumnDefs(flattenedColumns, region);

  return builtColumns;
};

const buildColumnsForRegion = (region, colDepth) => {
  const transformedColumns = buildDynamicColumns(region, colDepth);
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
