const { flatten, getUniqueRegions } = require('./util');
const {
  addRowKeysToRows,
  addFieldToRows,
  buildRowDef,
  buildRows,
  getRowKeys
} = require('./row');
const {
  buildColumnsForRegion,
  getInnermostColumns,
  addVariationColumns
} = require('./column');
const { buildRowColumns } = require('./rowColumn');
const { makeLowerCase } = require('./../util');

const transformRows = (columnDefs, regions, columnRowDefs, rowDepth, transforms) => {
  const rowDef = buildRowDef(columnDefs);
  let rowDefs = buildRows(regions, rowDef, rowDepth);

  const rowKeys = getRowKeys(columnRowDefs);
  // NOTE: This is a bit procedural which I am still working out how I feel about it ~LR
  addFieldToRows(rowDefs, rowKeys);
  addRowKeysToRows(rowDefs);

  return {
    colDefs: columnDefs,
    rowDefs: rowDefs,
    transforms
  };
};

const processRegion = (region, colDepth) => {
  const innnerColumns = getInnermostColumns(region.columns);
  addVariationColumns(region, innnerColumns);
  return buildColumnsForRegion(region, colDepth);
};

const transformColumns = (regions, colDepth, rowDepth, transforms) => {
  const uniqueRegions = getUniqueRegions(regions, 'colIndex');
  const columnRowDefs = buildRowColumns(uniqueRegions[0], colDepth, rowDepth);
  const columns = uniqueRegions.map(region => processRegion(region, colDepth));
  const columnDefs = assembleColumns(columnRowDefs, columns, uniqueRegions[0]);

  return transformRows(
    columnDefs,
    regions,
    columnRowDefs,
    rowDepth,
    transforms
  );
};

// NOTE: This is a placeholder function that will evolve
const assembleColumns = (columnRowDefs, columns, firstRegion) => {
  // TODO: replace with something better. Need to do more testing until it breaks (nested rows) and go from there.
  // Stomp on columnRowDefs to use descriptions from manifest. 
  // Can't do this earlier because it throws off all of the data stitching functions.
  columnRowDefs = [{"columns":[{"value":"","level":0}],"properties":{"field":"description","editable":true}}];
  
  if (firstRegion.includeVariance) {
    // PERIODIC LAYOUT
    return [...columns[0], ...columnRowDefs, ...columns[1]];
  } else {
    // ALL THE COLUMNS
    return [...columnRowDefs, ...flatten(columns, 1)];
  }
};

const getNecessaryTransforms = (regions, colDepth, rowDepth) => {
  let transforms = [];
  regions.forEach(region => transforms.push(region.transform));
  return transformColumns(regions, colDepth, rowDepth, transforms);
};

const buildTableData = manifest => {
  if (!manifest.hasOwnProperty('regions')) {
    throw new Error('Manfiest must contain regions');
  }

  const regions = manifest.regions;
  const firstRegion = regions[0];
  const colDepth = firstRegion.colDepth;
  const rowDepth = firstRegion.rowDepth;

  return getNecessaryTransforms(regions, colDepth, rowDepth);
};

module.exports = { buildTableData };
