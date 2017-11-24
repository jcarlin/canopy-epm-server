const { flatten, completelyFlatten, getUniqueRegions } = require('./util');

const getRowKeys = columnRowDefs => {
  return columnRowDefs.map(rowDef => rowDef.properties.field);
};

const addRowKeysToRows = rowDefs => {
  rowDefs.forEach(rowDef => {
    Object.keys(rowDef).forEach(key => {
      if (typeof rowDef[key] === 'object') {
        rowDef[key]['rowKey'] = rowDef.field;
      }
    });
  });
};

const addFieldToRows = (rowDefs, rowKeys) => {
  rowDefs.forEach(rowDef => {
    let field = generateRowKey(rowKeys, rowDef);
    rowDef.field = field;
  });
};

const buildRowDef = columnDefs => {
  return columnDefs.reduce((acc, cur) => {
    return Object.assign(acc, {
      [cur.properties.field]: {
        value: 0,
        rowKey: '',
        columnKey: generateColumnKey(cur.columns),
        rowIndex: cur.properties.rowIndex,
        colIndex: cur.properties.colIndex,
        editable: cur.properties.editable
      }
    });
  }, {});
};

const generateRowKey = (rowKeys, rowDef) => {
  return rowKeys.map(key => `${key}_${rowDef[key]}`).join('__');
};

const generateColumnKey = columns => {
  return columns
    .map(column => `${column.dimension}_${column.value}`)
    .join('__');
};

const buildRowDefs = (rows, rowDef) => {
  return rows
    .map(rows => rows.map(row => ({ [row.dimension]: row.member, editable: row['data entry'] })))
    .map(rows =>
      rows.reduce((acc, cur) => {
        const row = Object.assign({}, rowDef, acc, cur);
        return JSON.parse(JSON.stringify(row)); // Deep clone
      }, {})
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

module.exports = {
  addRowKeysToRows,
  addFieldToRows,
  buildRowDef,
  buildRows,
  getRowKeys
};
