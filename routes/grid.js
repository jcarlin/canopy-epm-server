const Router = require('express-promise-router')
const manifests = require('./../manifests')
const grid = require('./../grid')
const util = require('./../util')
const transforms = require('./../transforms')
const db = require('../db')
const fs = require('fs')
const _ = require('lodash')
const hrtime = require('process.hrtime')

const router = new Router()

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
  
  //const totalTimeTimer = hrtime()
  let queryTimes = []
  let stitchTimes = []
  let totalTimes = []
  promMap = manifest.regions.map(async (region) => {
    const totalTimeTimer = hrtime()
    const pinned = grid.getPinnedSet(region.pinned);
    const includeVariance = region.includeVariance;
    const includeVariancePct = region.includeVariancePct;
    const dimKeys = JSON.parse(process.env.DIM_KEYS);
    const dimensionsWithKeys = util.mergeDimKeys(pinned, dimKeys);
    const dimIdSql = transforms.getDimensionIdSql(dimensionsWithKeys);
    const transform = await util.readJsonFile(`./transforms/${region.transform}`);
    const dimIds = await db.query(dimIdSql);
    const sql = await buildDataSetQuery(dimensionsWithKeys, dimIds, transform);

    const execTimer = hrtime()
    const data = await db.query(sql);
    queryTimes.push(hrtime(execTimer, 'ms'))

    console.time('stitchData');
    const stitchTimer = hrtime()
    const stitched = await stitchData(data, region);
    stitchTimes.push(hrtime(stitchTimer, 'ms'))
    console.timeEnd('stitchData');
    
    totalTimes.push(hrtime(totalTimeTimer, 'ms'))
    return stitched;
  });

  await Promise.all(promMap).then((tableDataArr) => {
    let totalTime = totalTimes.reduce((total, curr) => {return total + curr});
    let totalQueryTime = queryTimes.reduce((total, curr) => {return total + curr});
    let totalRenderTime = totalTime - totalQueryTime;
    totalRenderTime = util.formatTimeStat(totalRenderTime);
    totalQueryTime = util.formatTimeStat(totalQueryTime);
    totalTime = util.formatTimeStat(totalTime);
    
    // return the last tableData object - this one will have all the values
    const tableData = tableDataArr[tableDataArr.length -1];
    tableData.statistics = {
      queryTime: totalQueryTime,
      renderTime: totalRenderTime,
      totalTime: totalTime
      // more stats can go here
    };
    return res.json(tableData);
  });
});

/**
 * Edit a cell based on an ICE
 */
router.patch('/', async (req, res, next) => {
  if (!req.body.ice || !req.body.manifest) {
    return res.status(400).json({
      error: 'You must send data and manifest for independent change event'
    });
  }
  
  let sql = null;
  const ice = req.body.ice;
  const manifest = req.body.manifest;
  const newValue = ice.value;
  const region = manifest.regions.find(region => {
    return region.colIndex === ice.colIndex && region.rowIndex === ice.rowIndex;
  });
  const keySets = util.buildKeySet(grid.extractKeySetAndId(ice.rowKey), grid.extractKeySetAndId(ice.columnKey), grid.getPinnedSet(region.pinned));
  const dimKeys = JSON.parse(process.env.DIM_KEYS);
  const dimensionsWithKeys = util.mergeDimKeys(keySets, dimKeys);

  const transform = await util.readJsonFile(`./transforms/${region.transform}`)
  const dimIdSql = transforms.getDimensionIdSql(dimensionsWithKeys);
  const dimData = await db.query(dimIdSql)
  const factKeys = JSON.parse(process.env.FACT_KEYS);
  const factInfo = util.mergeFactKeys(transform.metrics, factKeys)[0];
  const dimensionsWithVals = util.mergeDimVals(dimensionsWithKeys, dimData[0]);
  
  const sqlParams = {
    tableName: `root_${factInfo.fact_id}`,
    factId: factInfo.fact_id,
    metric: transform.metrics[0],
    newValue: ice.value,
    filterStatements: dimensionsWithVals.map(dim => {
      return dim.idWhereClause;
    }).join(' AND '),
    dimIdValues: dimensionsWithVals.map(dim => { return dim.value; }),
    dimIdColumns: dimensionsWithVals.map(dim => { return dim.idColName; }),
    dimRIdColumns: dimensionsWithVals.map(dim => { return `r.${dim.idColName}`; }),
    dimRIdValues: dimensionsWithVals.map(dim => { return `${dim.value} AS ${dim.idColName}`; })
  };

  let results = null;
  if (process.env.DB_TYPE === 'POSTGRESQL') {
    sql = transforms.deactivateSql(sqlParams);
    await db.query(sql)
    await db.query(transforms.insertSql(sqlParams))
    await db.query(transforms.updateBranch15NatJoinSql(sqlParams))
    results = await db.query(transforms.updateApp20NatJoinSql(sqlParams))
  } else if (process.env.DB_TYPE === 'SNOWFLAKE') {
    results = await db.query(transforms.sfInsertSql(sqlParams))
  }
  return res.json({results})
});

// export our router to be mounted by the parent application
module.exports = router
