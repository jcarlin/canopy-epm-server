const getCellData = (dataMap, rowKey, colKey) => {
  return dataMap[rowKey][colKey];
};

const setCellData = (dataMap, rowKey, colKey, cellData) => {
  dataMap[rowKey][colKey] = cellData;
};

module.exports = { getCellData, setCellData };
