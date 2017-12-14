const { flatten, completelyFlatten, getUniqueRegions } = require('./util');

const buildRows = (regions, rowDef, depth) => {
  const transformedRows = transformRowDefs(regions);
  const flattenedRows   = flatten(transformedRows, depth - 1);
  const builtRows       = buildRowDefs(flattenedRows, rowDef);

  // Set the editable field based on row and column definitions
  calculateEditableCells(builtRows);

  return completelyFlatten(builtRows);
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

const buildRowDefs = (rows, rowDef) => {
  return rows
    .map(rows =>
      rows.map(row => {
        return {
          [row.dimension]: row.member,
          editable: row['data entry']
        };
      })
    )
    .map(rows => [Object.assign(...rows)]) // This is some serious ES6 voodoo
    .map(rows => {
      return rows.reduce((acc, cur) => {
        const row = Object.assign({}, acc, rowDef, cur);
        return JSON.parse(JSON.stringify(row)); // Deep clone
      }, {});
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

// -------------------------------------------------------------------
// UTILITY FUNCTIONS
// -------------------------------------------------------------------
const calculateEditableCells = rows => rows.forEach(row => {
  Object.keys(row).forEach(key => {
    if (typeof row[key] === 'object') {
      // Doing this longhand for documentation purposes ~LR
      const colEditable = row[key]['editable'];
      const rowEditable = row['editable'];
      row[key]['editable'] = colEditable && rowEditable;
    }
  });
});

const addFieldToRows = (rowDefs, rowKeys) => {
  rowDefs.forEach(rowDef => {
    let field = generateRowKey(rowKeys, rowDef);
    rowDef.field = field;
  });
};

const generateRowKey = (rowKeys, rowDef) => {
  return rowKeys.map(key => `${key}__${rowDef[key]}`).join('___');
};

const generateColumnKey = columns => {
  return columns
    .map(column => `${column.dimension}__${column.value}`)
    .join('___');
};

const getRowKeys = columnRowDefs => {
  return columnRowDefs.map(rowDef => rowDef.properties.field);
};

const addRowKeysToRows = rowDefs => {
  rowDefs.forEach(rowDef => {
    Object
      .keys(rowDef)
      .forEach(key => {
        if (typeof rowDef[key] === 'object') {
          rowDef[key]['rowKey'] = rowDef.field;
        }
      });
  });
};

module.exports = {
  addRowKeysToRows,
  addFieldToRows,
  buildRowDef,
  buildRows,
  getRowKeys
};
