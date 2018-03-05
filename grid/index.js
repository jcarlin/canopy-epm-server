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
  let loggedToConsole = true;

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
          console.log('stitchDataBaseData() ... ');
          console.log('colIndex: ' + JSON.stringify(colIndex));
          console.log('rowIndex: ' + JSON.stringify(rowIndex));
          console.log('columnKeys: ' + JSON.stringify(columnKeys));
          console.log('rowKeys: ' + JSON.stringify(rowKeys));
          console.log('rowKeyStrings: ' + JSON.stringify(rowKeyStrings));
          console.log('columnKeyStrings: ' + JSON.stringify(columnKeyStrings));
          console.log('joinedColumnKeys: ' + JSON.stringify(joinedColumnKeys));
          console.log('joinedRowKeys: ' + JSON.stringify(joinedRowKeys));
          console.log('totalMatchString: ' + JSON.stringify(totalMatchString));
          loggedToConsole = true;
        }

        // Dimension from pinned[0]
        const pinned = findPinned(manifest.regions, colIndex, rowIndex);
      
        match = dbData.find(row => {
          return eval(totalMatchString);
        });
  
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

const stitchDatabaseRegionData = (region, tableData, dbData) => {
  let loggedToConsole = true;

  tableData.rowDefs.forEach(def => {
    const keys = Object.keys(def);

    let matches;
  
    keys.forEach(key => {
      if (typeof def[key] === 'object') {
        const colIndex = def[key].colIndex;
        const rowIndex = def[key].rowIndex;
        const columnKeys = extractKeySet(def[key].columnKey);
        const rowKeys = extractKeySet(def[key].rowKey);

        // debug
        if (!loggedToConsole) {
          console.log('stitchDataBaseData() ... ');
          console.log('colIndex: ' + JSON.stringify(colIndex));
          console.log('rowIndex: ' + JSON.stringify(rowIndex));
          console.log('columnKeys: ' + JSON.stringify(columnKeys));
          console.log('rowKeys: ' + JSON.stringify(rowKeys));
          loggedToConsole = true;
        }

        // console.time('match')
        match = dbData.find((row) => {
          const rowMatch = rowKeys.every(rowKey => row[rowKey.dimension] === rowKey.member)
          const colMatch = columnKeys.every(columnKey => row[columnKey.dimension] === columnKey.member)
          return rowMatch && colMatch;
        });
        // console.timeEnd('match')

        // Set the value (number) of the cell
        if (match) {
          // Put any desired rounding here. Currently handled in the client: https://goo.gl/J7CKFp
          def[key].value = match[region.pinned[0].member];
        } else {
          def[key].value = (def[key].value ? def[key].value : null);
        }
      }
    });
  });
  return tableData;
};

const getPinnedSet = pinned => {
  return pinned.filter(pin => pin.dimension !== 'Metric').map(pin => {
    return {
      dimension: makeLowerCase(pin.dimension),
      member: pin.member
    };
  });
};

module.exports = { extractKeySet, extractKeySetAndId, stitchDatabaseData, stitchDatabaseRegionData, produceVariance, getPinnedSet };
