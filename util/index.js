const capitalize = word => {
  return word.charAt(0).toUpperCase() + word.slice(1);
};

const makeLowerCase = word => {
  return word.charAt(0).toLowerCase() + word.slice(1);
};

// Given a dataset containing rows,
// extract the dimension and member
// give give dimension:member
const extractFields = elements => {
  return elements.map(element => {
    if (!element.dimension || !element.member) return;
    return {
      [makeLowerCase(element.dimension)]: element.member
    };
  });
};

const buildElements = (oldElements, newElements) => {
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

const seekElements = (elements, type, built = []) => {
  let children = elements[type];
  let builtElements;
  if (children) {
    builtElements = buildElements(built, extractFields(children));
    const newBuild = [
      Object.assign({}, ...extractFields(children), ...builtElements)
    ];
    for (var i = 0, len = children.length; i < len; i++) {
      if (children[i].hasOwnProperty(type)) {
        return seekElements(children[i], type, newBuild);
      } else {
        return builtElements;
      }
    }
  }
};

module.exports = { capitalize, extractFields, buildElements, seekElements };
