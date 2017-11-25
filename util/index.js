const capitalize = word => {
  return word.charAt(0).toUpperCase() + word.slice(1);
};

const makeLowerCase = word => {
  return word.toLowerCase();
};

// Given a dataset containing rows,
// extract the dimension and member
// give give dimension:member
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

module.exports = {
  capitalize,
  makeLowerCase,
  extractFields,
  buildElements,
  seekElements,
  getExtractedElements
};
