const { flatten, completelyFlatten, getUniqueRegions } = require('./util');

const getRowKeys = columnRowDefs => {
  return columnRowDefs.map(rowDef => rowDef.properties.field);
};

const addFieldToRows = (rowDefs, rowKeys) => {
  const addedFields = rowDefs.map(rowDef => {
    const field = rowKeys.map(key => rowDef[key]).join('_');
    return Object.assign({}, rowDef, { field });
  });
  return addedFields;
};

const buildRowDef = columnDefs => {
  return columnDefs.reduce((acc, cur) => {
    return Object.assign(acc, {
      [cur.properties.field]: {
        value: 0,
        editable: cur.properties.editable
      }
    });
  }, {});
};

const buildRowDefs = (rows, rowDef) => {
  return rows
    .map(rows => rows.map(row => ({ [row.dimension]: row.member })))
    .map(rows =>
      rows.reduce((acc, cur) => Object.assign({}, rowDef, acc, cur), {})
    );
};
const flattenRowDefs = (rows, depth) => {
  return flatten(rows, depth - 1);
};

const transformRowDefs = regions => {
  const newRegions = getUniqueRegions(regions, 'rowIndex');
  const rows = completelyFlatten(newRegions.map(region => region.rows));
  const transform = (row, parents) => {
    parents = [...parents, row];
    return row.rows ? row.rows.map(r => transform(r, parents)) : parents;
  };

  return rows.map(row => transform(row, []));
};

const buildRows = (regions, rowDef, depth) => {
  const transformedRows = transformRowDefs(regions);
  const flattenedRows = flattenRowDefs(transformedRows, depth);
  const builtRows = buildRowDefs(flattenedRows, rowDef);

  return completelyFlatten(builtRows);
};

module.exports = { addFieldToRows, buildRowDef, buildRows, getRowKeys };
