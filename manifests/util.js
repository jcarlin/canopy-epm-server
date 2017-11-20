const flatten = (collection, depth) => {
  const flatter = (items, depth) => {
    if (depth > 0) {
      return flatter([].concat.apply([], items), (depth -= 1));
    } else {
      return items;
    }
  };
  return flatter(collection, depth);
};

const getUniqueRegions = (regions, key) => {
  const flags = {};
  const newRegions = regions.filter(region => {
    if (flags[region[key]]) {
      return false;
    }
    flags[region[key]] = true;
    return true;
  });

  return newRegions;
};

const completelyFlatten = items => {
  const flat = [];

  items.forEach(item => {
    if (Array.isArray(item)) {
      flat.push(...completelyFlatten(item));
    } else {
      flat.push(item);
    }
  });

  return flat;
};

module.exports = { flatten, completelyFlatten, getUniqueRegions };
