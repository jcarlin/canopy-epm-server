const capitalize = word => {
  return word.charAt(0).toUpperCase() + word.slice(1);
};

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
      return dimKey.name === dim.dimension;
    });

    dim.id = dimInfo.id;
    dim.idColName = `d${dimInfo.id}_id`;

    return dim;
  });
}; 

  // return dimArray.map(dim => {
  //   // Get matching dimension info
  //   const dimInfo = dimKeyArray.find(dimKey => {
  //     console.log("dimKey: ", dimKey);
  //     console.log("dim: ", dim);
  //     return dimKey.name.toString() === dim.toString();
  //   });

  //   return Object.assign(
  //     {}, 
  //     {dimension: dim}, 
  //     {id: dimInfo.id},
  //     {idColName: `d${dimInfo.id}_id`}
  //   );
  // });

// Return factKey from grainDefs.factKeys (because it has more info) instead of fact from transform.dimensions
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
  buildKeySet
};
