const path = require('path');
const fs = require('fs');

const capitalize = word => {
  return word.charAt(0).toUpperCase() + word.slice(1);
};

const formatTimeStat = msDuration => {
  const milliseconds = parseInt(msDuration%1000)
  let seconds = parseInt((msDuration/1000)%60)
  let minutes = parseInt((msDuration/(1000*60))%60);

  minutes = (minutes < 10) ? "0" + minutes : minutes;
  seconds = (seconds < 10) ? "0" + seconds : seconds;

  return seconds + "s " + milliseconds + 'ms';
}

const makeLowerCase = word => {
  return word.toLowerCase();
};

// Given a dataset containing rows,
// extract the dimension and member
// to give dimension:member
const extractFields = elements => {
  return elements.map(element => {
    if (!element.dimension || !element.member) return;
    let output = {};
    if (element.hasOwnProperty('data entry')) {
      output = {
        [makeLowerCase(element.dimension)]: element.member,
        editable: element['data entry']
      };
    } else {
      output = {
        [makeLowerCase(element.dimension)]: element.member
      };
    }
    return output;
  });
};

const extractColumns = columns => {
  return columns.map(column => {
    if (!column.dimension || !column.member) return;
    let output;
    if (column.hasOwnProperty('data entry')) {
      output = {
        [makeLowerCase(column.dimension)]: {
          value: column.member,
          level: column.level
        },
        editable: column['data entry']
      };
    } else {
      output = {
        [makeLowerCase(column.dimension)]: {
          value: column.member,
          level: column.level
        }
      };
    }
    return output;
  });
};

const buildElements = (oldElements, newElements, count) => {
  let output = [];
  newElements.forEach(newElement => {
    oldElements.forEach(oldElement => {
      output.push({
        ...newElement,
        ...oldElement
      });
    });
  });

  return output;
};

const seekElements = (elements, type, built = [], count = 0) => {
  let children = elements[type];
  let builtElements;
  let newBuild;
  if (children) {
    if (type === 'columns') {
      builtElements = buildElements(built, extractColumns(children), count);
      newBuild = [
        Object.assign({}, ...extractColumns(children), ...builtElements)
      ];
    } else {
      builtElements = buildElements(built, extractFields(children), count);
      newBuild = [
        Object.assign({}, ...extractFields(children), ...builtElements)
      ];
    }
    for (var i = 0, len = children.length; i < len; i++) {
      if (children[i].hasOwnProperty(type)) {
        return seekElements(children[i], type, newBuild, ++count);
      } else {
        return builtElements;
      }
    }
  }
};

const getExtractedElements = (manifest, type) => {
  if (!manifest) throw new Error('A manifest must be supplied');
  if (!manifest.hasOwnProperty('regions'))
    throw new Error('Manifest must have regions');
  let output = [];
  manifest.regions.forEach(region => {
    if (type === 'columns') {
      output.push({
        [type]: seekElements(region, type),
        rowIndex: region.rowIndex,
        colIndex: region.colIndex,
        showRowHeaders: region.showRowHeaders,
        showColHeaders: region.showColHeaders,
        metric: region.pinned[0].member
      });
    } else if (type === 'rows') {
      output.push({
        [type]: seekElements(region, type),
        rowIndex: region.rowIndex,
        colIndex: region.colIndex,
        showRowHeaders: region.showRowHeaders,
        showColHeaders: region.showColHeaders
      });
    }
  });
  return output;
};

// Add id and idColName to the dimensions (objects) array
const mergeDimKeys = (dimArray, dimKeys) => {
  return dimArray.map(dim => {
    // Get dimension info
    const dimInfo = dimKeys.find(dimKey => {
      return dimKey.dim_name === dim.dimension;
    });

    dim.id = dimInfo.dim_id;
    dim.idColName = `d${dimInfo.dim_id}_id`;

    return dim;
  });
};

// Return factKey from graindefs.factKeys (because it has more info) instead of fact from transform.dimensions
const mergeFactKeys = (factArray, factKeyArray) => {
  return factArray.map(fact => {
    return factKeyArray.find(factKey => {
      return factKey.fact_name === fact;
    });
  });
};

// Add value and idWhereClause to the dimensions (objects) array
const mergeDimVals = (dimensions, dimVals) => {
  return dimensions.map(dim => {
    const dimVal = dimVals[dim.idColName];
    return Object.assign(
      {},
      dim,
      {value: dimVal},
      {idWhereClause: `d${dim.id}_id = ${dimVal}`}
    );
  })
};

const buildKeySet = (rowKeySet, colKeySet, pinned) => {
  return [
    ...rowKeySet,
    ...colKeySet,
    ...pinned
  ];
};

const readDir = filePath => new Promise((resolve, reject) => {
  fs.readdir(filePath, (err, files) => {
    if (err) reject(err);
    else resolve(files);
  });
});

const readJsonFile = filePath => new Promise((resolve, reject) => {
  fs.readFile(filePath, (err, data) => {
    if (err) reject(err);
    else resolve(JSON.parse(data));
  });
});

const writeJsonFile = (filePath, data) => new Promise((resolve, reject) => {
  fs.writeFile(filePath, data, (err) => {
    if (err) reject(err);
    else resolve('The file has been saved!');
  });
});

const readFile = filePath => new Promise((resolve, reject) => {
  fs.readFile(filePath, (err, data) => {
    if (err) reject(err);
    else resolve(data.toString());
  });
});

const removeDuplicates = (myArr, prop, nestedProp) => {
  return myArr.filter((obj, pos, arr) => {
      return arr.map(mapObj => mapObj[prop][nestedProp]).indexOf(obj[prop][nestedProp]) === pos;
  });
};

module.exports = {
  capitalize,
  makeLowerCase,
  extractFields,
  buildElements,
  seekElements,
  getExtractedElements,
  mergeDimKeys,
  mergeFactKeys,
  mergeDimVals,
  buildKeySet,
  readDir,
  readFile,
  readJsonFile,
  removeDuplicates,
  formatTimeStat,
  writeJsonFile
};
