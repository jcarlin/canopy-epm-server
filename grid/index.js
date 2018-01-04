const debug = require('debug')('log');
const { makeLowerCase } = require('./../util');

const extractKeySet = rawKey => {
  return rawKey.split('___').map(keyGroup => {
    const keySet = keyGroup.split('__');
    return {
      dimension: makeLowerCase(keySet[0]),
      member: keySet[1]
    };
  });
};

// TODO: remove this hack by first updating the manifest dimension's value.
// i.e. 'time_id' -> 'time'
const extractKeySetAndId = rawKey => {
  return rawKey.split('___').map(keyGroup => {
    const keySet = keyGroup.split('__');
    let dim = null;

    if (keySet[0].match('_id')) {
      dim = keySet[0].substring(0, keySet[0].length -3);
    }
    return {
      dimension: dim || makeLowerCase(keySet[0]),
      member: keySet[1]
    };
  });
};

const produceVariance = tableData => {
  tableData.rowDefs.forEach(def => {
    const keys = Object.keys(def);
    let keyBag = [];
    keys.forEach((key, i) => {
      keyBag.push(key);
      if (/Variance/.test(def[key].columnKey)) {
        def[key].value = def[keyBag[i - 1]].value - def[keyBag[i - 2]].value;
      }
      if (/Variance %/.test(def[key].columnKey)) {
        def[key].value =
          def[keyBag[i - 1]].value / def[keyBag[i - 2]].value * 100;
      }
    });
  });
  return tableData;
};

const produceKeyStrings = keys => {
  return keys.map(key => {
    return `row.${key.dimension} === '${key.member}'`;
  });
};

const findPinned = (regions, colIndex, rowIndex) => {
  return regions.find(region => {
    return region.colIndex === colIndex && region.rowIndex === rowIndex;
  }).pinned;
};

const stitchDatabaseData = (manifest, tableData, dbData) => {
  let loggedToConsole = false;

  tableData.rowDefs.forEach(def => {
    const keys = Object.keys(def);

    keys.forEach(key => {
      if (typeof def[key] === 'object') {
        const colIndex = def[key].colIndex;
        const rowIndex = def[key].rowIndex;
        const columnKeys = extractKeySet(def[key].columnKey);
        const rowKeys = extractKeySet(def[key].rowKey);
        const rowKeyStrings = produceKeyStrings(rowKeys);
        const columnKeyStrings = produceKeyStrings(columnKeys);
        const joinedColumnKeys = columnKeyStrings.join(' && ');
        const joinedRowKeys = rowKeyStrings.join(' && ');
        const totalMatchString = `${joinedColumnKeys} && ${joinedRowKeys}`;

        // debug
        if (!loggedToConsole) {
          debug('stitchDataBaseData() ... ');
          debug('colIndex: ' + JSON.stringify(colIndex));
          debug('rowIndex: ' + JSON.stringify(rowIndex));
          debug('columnKeys: ' + JSON.stringify(columnKeys));
          debug('rowKeys: ' + JSON.stringify(rowKeys));
          debug('rowKeyStrings: ' + JSON.stringify(rowKeyStrings));
          debug('columnKeyStrings: ' + JSON.stringify(columnKeyStrings));
          debug('joinedColumnKeys: ' + JSON.stringify(joinedColumnKeys));
          debug('joinedRowKeys: ' + JSON.stringify(joinedRowKeys));
          debug('totalMatchString: ' + JSON.stringify(totalMatchString));
          loggedToConsole = true;
        }

        const pinned = findPinned(manifest.regions, colIndex, rowIndex);
        let match;

        if (dbData.rows) {
          match = dbData.rows.find(row => {
            return eval(totalMatchString);
          });
        } else {
          match = dbData.find(row => {
            return eval(totalMatchString);
          });
        }
        

        match
          ? (def[key].value = match[pinned[0].member])
          : (def[key].value = null);

        // Set the value (number) of the cell
        if (match) {
          // Put any desired rounding here. Currently handled in the client: https://goo.gl/J7CKFp
          def[key].value = match[pinned[0].member];
        } else {
          def[key].value = null;
        }
      }
    });
  });
  return tableData;
};

module.exports = { extractKeySet, extractKeySetAndId, stitchDatabaseData, produceVariance };
