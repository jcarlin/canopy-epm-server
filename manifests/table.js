const { flatten, getUniqueRegions } = require('./util');
const { addFieldToRows, buildRowDef, buildRows, getRowKeys } = require('./row');
const {
  buildColumnsForRegion,
  getInnermostColumns,
  addVariationColumns
} = require('./column');
const { buildRowColumns } = require('./rowColumn');

const buildDataMap = rowDefs => {
  return rowDefs.reduce((acc, cur) => {
    acc[cur.field] = {};
    Object.keys(cur).map(key => {
      if (typeof cur[key] === 'object') {
        return (acc[cur.field][key] = cur[key]);
      }
    });

    return acc;
  }, {});
};

const transformRows = (columnDefs, regions, columnRowDefs, rowDepth) => {
  const rowDef = buildRowDef(columnDefs);
  let rowDefs = buildRows(regions, rowDef, rowDepth);

  // console.log('row before', rowDefs);
  const rowKeys = getRowKeys(columnRowDefs);
  console.log('row keys', rowKeys);
  // NOTE: This is a bit procedural which I am still working out how I feel about it ~LR
  rowDefs = addFieldToRows(rowDefs, rowKeys);
  const dataMap = buildDataMap(rowDefs);
  // console.log('row after', rowDefs);
  return {
    colDefs: columnDefs,
    rowDefs: rowDefs
  };
};

const processRegion = region => {
  const innnerColumns = getInnermostColumns(region.columns);
  const variantColumns = addVariationColumns(region, innnerColumns);
  return buildColumnsForRegion(region);
};

const transformColumns = (regions, colDepth, rowDepth) => {
  const uniqueRegions = getUniqueRegions(regions, 'colIndex');
  const columnRowDefs = buildRowColumns(uniqueRegions[0], colDepth, rowDepth);
  const columns = uniqueRegions.map(region => processRegion(region));

  // ALL THE COLUMNS
  const columnDefs = [...columnRowDefs, ...flatten(columns, 1)];

  // PERIODIC LAYOUT
  // this.columnDefs  = [...columns[0], ...this.columnRowDefs, ...columns[1]];

  return transformRows(columnDefs, regions, columnRowDefs, rowDepth);
};

const buildTableData = manifest => {
  if (!manifest.hasOwnProperty('regions')) {
    throw new Error('Manfiest must contain regions');
  }

  const regions = manifest.regions;

  const firstRegion = regions[0];
  const colDepth = firstRegion.colDepth;
  const rowDepth = firstRegion.rowDepth;

  return transformColumns(regions, colDepth, rowDepth);
};

const setTableData = tableData => {
  this.tableData = tableData;
};

const getTableData = () => {
  return this.tableData;
};

module.exports = { buildTableData };
