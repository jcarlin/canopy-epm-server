const Router = require('express-promise-router')
const manifests = require('./../manifests')
const grid = require('./../grid')
const util = require('./../util')
const transforms = require('./../transforms')
const db = require('../db')
const fs = require('fs');
const _ = require('lodash');

// create a new express-promise-router
// this has the same API as the normal express router except
// it allows you to use async functions as route handlers
const router = new Router()

let graindefs = {};
let dimKeys = {};
let factKeys = [];

(async () => {
  graindefs = await util.readFile('./graindefs/graindefs.json');
  dimKeys = graindefs.dimKeys;
  factKeys = graindefs.factKeys;
})();

/**
 * In response to the client app sending
 * a hydrated manifest, send back the completely
 * built table data ready to be consumed by ag-grid.
 */
router.post('/', async (req, res, next) => {
  if (!req.body.manifest) {
    return res.status(400).json({
      error: `You must supply a manifest. 
        Send it on an object with a 'manfifest' key: { manifest: ... }`
    });
  }

  const tableDataSet = [];
  const colIndexes = [];
  const rowIndexes = [];
  let compColDefs = [];
  let compRowDefs = [];
  const manifest = req.body.manifest;
  console.time('buildTableData');
  let tableData = manifests.buildTableData(manifest);
  console.timeEnd('buildTableData');

  const buildDataSetQuery = (dimensionsWithKeys, dimIds, transform) => {
    return new Promise((resolve, reject) => {
      const dimensionsWithVals = util.mergeDimVals(dimensionsWithKeys, dimIds[0]);
      const filterStatements = dimensionsWithVals.map(dim => {
        return dim.idWhereClause;
      }).join(' AND ');
      
      const sqlParams = {
        dimensions: transform.dimensions,
        metrics: transform.metrics.map(metric => `"${metric}"`).join(','),
        tableName: transform.table,
        filterStatements: filterStatements
      };
      
      // Set params according to db nuances
      if (process.env.DB_TYPE === 'SNOWFLAKE') {
        sqlParams.metrics = transform.metrics.map(metric => `"${metric.toUpperCase().split(' ').join('_')}"`).join(',');
        sqlParams.tableName = transform.table.split(' ').join('_');
      }

      resolve(transforms.querySql(sqlParams));
    });
  };

  // Format response object
  const stitchData = (dbData, region) => {
    return new Promise(resolve => {
      if (process.env.DB_TYPE === 'SNOWFLAKE') { 
        dbData = dbData.map(row => {
          return _.transform(row, function (res, val, key) {
            res[key.toLowerCase()] = val;
          }); 
        });
      }

      tableData = grid.stitchDatabaseRegionData(region, tableData, dbData);
      if (region.includeVariance && region.includeVariancePct) {
        // TODO - this might be broken
        tableData = grid.produceVariance(tableData);
      }
      resolve(tableData);
    });
  };

  promMap = manifest.regions.map(async (region) => {
    const pinned = grid.getPinnedSet(region.pinned);
    const includeVariance = region.includeVariance;
    const includeVariancePct = region.includeVariancePct;
    const dimensionsWithKeys = util.mergeDimKeys(pinned, dimKeys);
    const dimIdSql = transforms.getDimensionIdSql(dimensionsWithKeys);
    const transform = await util.readFile(`./transforms/${region.transform}`);
    // const dimIds = await getDimensionIds(dimensionsWithKeys);
    const dimIds = await db.query(dimIdSql);
    
    console.time('buildDataSetQuery');
    const sql = await buildDataSetQuery(dimensionsWithKeys, dimIds, transform);
    const data = await db.query(sql);
    console.timeEnd('buildDataSetQuery');
    
    console.time('stitchData');
    const stitched = await stitchData(data, region);
    console.timeEnd('stitchData');
    
    return stitched;
  });

  await Promise.all(promMap).then((tableDataArr) => {
    // return the last tableData object - this one will have all the values
    return res.json(tableDataArr[tableDataArr.length -1]);
  });
});

// export our router to be mounted by the parent application
module.exports = router
