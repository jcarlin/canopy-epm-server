require('dotenv').config();
const fs = require('fs');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');
const pg = require('pg');
const snowflake = require('snowflake-sdk');
const debug = require('debug')('log');
const async = require('async');
const _ = require('lodash');

const database = require('./database');
const { 
  makeGrainBlockQueryStrings,
  makeGrainBrickQueryStrings, 
  makeObjectCodeByTimeView, 
  makeAppNetRevView } = require('./graindefs/query.js');
const {
  capitalize,
  makeLowerCase,
  mergeDimKeys,
  mergeFactKeys,
  mergeDimVals,
  buildKeySet } = require('./util');
const {
  stitchDatabaseData, 
  produceVariance, 
  getPinnedSet, 
  extractKeySet, 
  extractKeySetAndId 
} = require('./grid');
const { buildTableData } = require('./manifests');

// Temporary defaulting to POSTGRESQL connection for all queries. UI will toggle this setting.
process.env.DATABASE = database.dbTypes.POSTGRESQL;
// process.env.DATABASE = database.dbTypes.SNOWFLAKE;

const port = process.env.PORT || 8080;
const app = express();

// TODO: replace this approach (reading from json file and storing in these vars) with something else
let grainDefs = {};
let dimKeys = {};
let factKeys = [];

// This will log to console if enabled (npm run-script dev)
debug('booting %o', 'debug');

// Define database clients/connections and method for querying
const pgClient = new pg.Client(database.getDbConnSettings(database.dbTypes.POSTGRESQL));
const sfClient = snowflake.createConnection(database.getDbConnSettings(database.dbTypes.SNOWFLAKE));

const dbClientQuery = (sql, callback) => {
  if (process.env.DATABASE == database.dbTypes.POSTGRESQL) {
    pgClient.query(sql, (err, data) => {
      callback(err, data ? data.rows : data);
    });
  } else if (process.env.DATABASE == database.dbTypes.SNOWFLAKE) {
    sfClient.execute({
      sqlText: sql,
      complete: function(err, stmt, data) {
        callback(err, data);
      }
    }); 
  }
};

// Necessary for express to be able to read the request body
app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true
  })
);

const checkJwt = jwt({
  secret: jwksRsa.expressJwtSecret({
    cache: true,
    rateLimit: true,
    jwksRequestsPerMinute: 5,
    jwksUri: `https://${process.env.AUTH0_ISSUER}/.well-known/jwks.json`
  }),
  audience: `${process.env.AUTH0_AUDIENCE}`,
  issuer: `https://${process.env.AUTH0_ISSUER}/`,
  algorithms: ['RS256']
});

// Allow cross-origin resource sharing
// NOTE: limit this to your own domain
// for prod use
app.use(cors());

// Use the checJwt middleware defined
// to ensure the user sends a valid
// access token produced by Auth0
// app.use(checkJwt);

const errorHandler = (err, req, res, next) => {
  debug('errorHandler err: ', err);
  res.status(500);
  res.json({ error: err });
};

/**
 * Express routes
 */
app.get('/database', (req, res, next) => {
  try {
    const dbMap = database.dbConnections.map(conn => {
      conn.active = (conn.type == process.env.DATABASE);
      return conn;
    });
    
    return res.json(dbMap);
  } catch(err) {
    next(err);
  }
});

app.post('/database', (req, res, next) => {
  try {
    if (!req.body.database || !database.dbTypes[req.body.database.toUpperCase()]) {
      return res.status(400).json({
        error: 'You must supply a database. Send it on an object with a `database` key: { database: ... }'
      });
    }
  
    process.env.DATABASE = database.dbTypes[req.body.database.toUpperCase()];
  
    const dbMap = database.dbConnections.map(conn => {
      conn.active = (conn.type == process.env.DATABASE);
      return conn;
    });
  
    console.log('Database switched to: ', req.body.database.toUpperCase());
    return res.json(dbMap);
  } catch(err) {
    next(err);
  }
});

/**
 * In response to the client app sending
 * a hydrated manifest, send back the completely
 * built table data ready to be consumed by ag-grid.
 */
app.post('/grid', (req, res, next) => {
  try {
    if (!req.body.manifest) {
      return res.status(400).json({
        error: `You must supply a manifest. 
          Send it on an object with a 'manfifest' key: { manifest: ... }`
      });
    }

    const db = Number(process.env.DATABASE); // Why is this converting to string??
    const manifest = req.body.manifest;
    const pinned = getPinnedSet(manifest.regions[0].pinned);
    const includeVariance = manifest.regions[0].includeVariance;
    const includeVariancePct = manifest.regions[0].includeVariancePct;
    const dimensionsWithKeys = mergeDimKeys(pinned, dimKeys);
    const tableData = buildTableData(manifest); // manifest -> something ag-grid can use
    let transform = null;

    // Get and parse transform
    const getTransform = callback => {
      fs.readFile(`./transforms/${tableData.transforms[0]}`, 'utf8', (err, data) => {
        if (err) callback(err);

        transform = JSON.parse(data);
        callback(null, transform);
      });
    };

    const getDimensionIds = (transform, callback) => {  
      sql = database.getDimensionIdSql(dimensionsWithKeys);
      dbClientQuery(sql, callback);
    };

    // Assemble sql for query
    const buildDataSetQuery = (result, callback) => {
      debug('buildDataSetQuery');
      const dimensionsWithVals = mergeDimVals(dimensionsWithKeys, result[0]);
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
      if (db === database.dbTypes.SNOWFLAKE) {
        sqlParams.metrics = transform.metrics.map(metric => `"${metric.toUpperCase().split(' ').join('_')}"`).join(',');
        sqlParams.tableName = transform.table.split(' ').join('_');
      }

      sql = database.querySql(sqlParams);
      dbClientQuery(sql, callback);
    };

    // Format response object
    const stitchData = (result, callback) => {
      if (process.env.DATABASE == database.dbTypes.SNOWFLAKE) {        
        result = result.map(row => {
          return _.transform(row, function (res, val, key) {
            res[key.toLowerCase()] = val;
          });
        });
      }
        
      let producedData = stitchDatabaseData(manifest, tableData, result);

      if (includeVariance && includeVariancePct) {
        producedData = produceVariance(producedData); 
      }
      callback(null, producedData);
    };

    // Process these functions in order, passing results to each subsequent function
    async.waterfall([
      getTransform,
      getDimensionIds,
      buildDataSetQuery,
      stitchData
    ],
    function(err, tableData) {
      if (err) {
        debug('async waterfall err: ', err);
        return next(err);
      }
      return res.json(tableData); // And, finally, return tableData
    });
  } catch (err) {
    return next(err);
  }
});

/**
 * Edit a cell by based on an ICE
 */
app.patch('/grid', (req, res, next) => {
  try {
    if (!req.body.ice || !req.body.manifest) {
      return res.status(400).json({
        error: 'You must send data and manifest for independent change event'
      });
    }
  
    let sql = null;
    let sqlParams = { };
    const ice = req.body.ice;
    const manifest = req.body.manifest;
    const newValue = ice.value;
    const region = manifest.regions.find(region => {
      return region.colIndex === ice.colIndex && region.rowIndex === ice.rowIndex;
    });
    const keySets = buildKeySet(extractKeySetAndId(ice.rowKey), extractKeySetAndId(ice.columnKey), getPinnedSet(region.pinned));
    const dimensionsWithKeys = mergeDimKeys(keySets, dimKeys);
  
    const getTransform = callback => {
      fs.readFile(`./transforms/${region.transform}`, 'utf8', (err, data) => {
        if (err) callback(err);
  
        transform = JSON.parse(data);
        callback(null, transform);
      });
    };

    const getDimensionIds = (transform, callback) => {  
      sql = database.getDimensionIdSql(dimensionsWithKeys);
      dbClientQuery(sql, callback);
    };
  
    const deactivateRecord = (result, callback) => {
      const factInfo = mergeFactKeys(transform.metrics, factKeys)[0];
      const dimensionsWithVals = mergeDimVals(dimensionsWithKeys, result[0]);
      
      sqlParams = {
        tableName: `root_${factInfo.fact_id}`,
        factId: factInfo.fact_id,
        metric: transform.metrics[0],
        newValue: ice.value,
        filterStatements: dimensionsWithVals.map(dim => {
          return dim.idWhereClause;
        }).join(' AND '),
        dimIdValues: dimensionsWithVals.map(dim => { return dim.value; }),
        dimIdColumns: dimensionsWithVals.map(dim => { return dim.idColName; })
      };

      sql = database.deactivateSql(sqlParams);
      dbClientQuery(sql, callback);
    };
  
    const insertNewValue = (result, callback) => {  
      dbClientQuery(database.insertSql(sqlParams), callback);
    };
  
    const updateBranch15 = (result, callback) => {
      if (process.env.DATABASE == database.dbTypes.POSTGRESQL) {
        sql = database.updateBranch15NatJoinSql(sqlParams);
      } else {
        sql = database.updateBranch15JoinSql(sqlParams);
      }
    
      dbClientQuery(sql, callback);
    };
  
    const updateApp20 = (result, callback) => {
      if (process.env.DATABASE == database.dbTypes.POSTGRESQL) {
        sql = database.updateApp20NatJoinSql(sqlParams);
      } else {
        sql = database.updateApp20JoinSql(sqlParams);
      }
    
      dbClientQuery(sql, callback);
    };
  
    // Process these functions in order, passing results to each subsequent function
    async.waterfall([
      getTransform,
      getDimensionIds,
      deactivateRecord,
      insertNewValue,
      updateBranch15
      // buildUpdateApp20
    ],
    function(err, results) {
      if (err) {
        debug('async waterfall err: ', err);
        return next(err);
      }
      return res.json({ results });
    });
  } catch(err) {
    return next(err);
  }
});

/**
 * Get an unhydrated manifest from the filesystem.
 * You must specify the type which corresponds to the
 * filename of the manifest on disk.
 */
app.get('/manifest', (req, res, next) => {
  try {
    const manifestType = req.query.manifestType;

    if (!manifestType) {
      return res.status(400).json({ error: 'You must supply a manifest type' });
    }

    fs.readFile(`./manifests/${manifestType}.json`, (err, data) => {
      if (err) {
        return res.status(400).json({ error: 'Manifest not found' });
      }
      const manifest = JSON.parse(data);
      return res.json({ manifest });
    });
  } catch(err) {
    next(err);
  }
});

app.get('/statistics', (req, res, next) => {
  try {
    const manifestType = req.query.manifestType;

    if (!manifestType) {
      return res.status(400).json({ error: 'You must supply a manifest type' });
    }

    fs.readFile(`./manifests/statistics.json`, (err, data) => {
      if (err) {
        return res.status(400).json({ error: 'Manifest not found' });
      }

      const statistics = JSON.parse(data);
        
      return res.json({
        "manifestStats": statistics.manifests[manifestType],
        "globalStats": statistics.global,
        "startTime": new Date()
      });
    });
  } catch(err) {
    next(err);
  }
});

// Utility endpoint which you can send
// a manifest to and receive the column and row defs
// produce by running it through `buildTableData`
app.post('/test-manifest', (req, res, next) => {
  try {
    if (!req.body.manifest) {
      return res.status(400).json({
        error:
          'You must supply a manifest. Send it on an object with a `manfifest` key: { manifest: ... }'
      });
    }

    const tableData = buildTableData(req.body.manifest);
    return res.json(tableData);
  } catch(err) {
    next(err);
  }
});

// Utility end point to output sql string of all graindef scripts
app.get('/grain', (req, res, next) => {
  try {
    return res.json({grainDefs});
  } catch (err) {
    next(err);
  }
});

/**
 * System admin utility route to handle creation of grain tables
 * 
 * 2018-01-03 run time: 5m7s
 */
app.post('/grain', (req, res, next) => {
  try {
    const grainSack = grainDefs.grainSack;
    const dimKeys = grainDefs.dimKeys;
    const hierKeys = grainDefs.hierKeys;
    let allQueryStrings = '';
    let tableCount = 0;

    // Cycle through the grainDefs array of grainDef objects and for each, cycle through it's memberSet array
    const grainDefsMap = () => {
      grainSack.map(grainDef => {
        const memberSets = mapMemberSets(grainDef);
      });
    };

    // Cycle through each memberSet array object
    const mapMemberSets = (grainDef) => {
      // Get diminfo from the dimInfo key, matched by memberSet's dimension
      grainDef.memberSets.map(member => {
        tableCount++;
        let queryStrings = "";

        // Get dimension info
        const dimInfo = dimKeys.find(dimKey => {
          return dimKey.name === member.dimension;
        });

        // params for the sql generation for this grainDef
        const sqlParams = {
          members: `'${member.members}'`,
          grainTableName: `grain_${grainDef.id}`,
          grainDefName: grainDef.name,
          grainDefId: grainDef.id,
          grainSerName: `gr${grainDef.id}_oid`,
          dimNumber: dimInfo.id,
          dimByte: dimInfo.byte === 2 ? 'SMALLINT' : 'INTEGER'
        };

        // Get hierarchy info
        if (member.hierarchy) {
          const hierInfo = hierKeys.find(hierKey => {
            return hierKey.name === member.hierarchy;
          });

          sqlParams.hierNumber = hierInfo.id;
          sqlParams.hierName = member.hierarchy;
        }
        
        // assemble the sql for the brick/block creation
        if (grainDef.grainType === "brick" && member.memberSetType === "evaluated") {
          queryStrings = makeGrainBrickQueryStrings(sqlParams);
        } else if (grainDef.grainType === "block") {
          // TODO: remove this hack (that grabs the parent grainDef (table))
          if (member.platformType === "node_leaf") {
            let parentDimNumber = grainDef.id -1;
            member.parentTableName = `grain_${parentDimNumber}`
          }

          // Extract object with database postgres
          if (member.memberSetType === "compute") {
            member.memberSetCode = member.memberSetCode.find(msc => {
              return msc.database === "postgres";
            });
          }
          queryStrings = makeGrainBlockQueryStrings(sqlParams, {"memberSet": member});
        }
        
        // Leaving out view sql for now
        // allQueryStrings = `${allQueryStrings}${queryStrings}${makeAppNetRevView()}${makeObjectCodeByTimeView()}`;
        
        allQueryStrings = `${allQueryStrings}${queryStrings}`;
      });
    };

    const execGrainSql = (sql, callback) => {
      pgClient.query(sql, (error, data) => {
        if (error) {
          console.log(error);
          return res.status(400).json({ error: 'Error writing to database.' + `${error}` });
        }
        return;
      });
    };
    
    // Async/await function that calls the above 3 functions
    const executeGrainDefSql = async () => {
      try {
        const gd = await grainDefsMap();
        const logSql = await console.log('graindef sql: ', allQueryStrings);
        const dbResults = await execGrainSql(allQueryStrings, (results) => {
          if (results == "error") {
            return res.status(400).json({ error: results });
          }

          return res.json({"tableCount": tableCount});
        });
      }
      catch(err) {
        console.log("/grain executeGrainDefSql error: ", err);
        return res.status(400).json({ error: err });
      }
    };

    executeGrainDefSql();
  } catch(err) {
    next(err);
  }
});

/**
 * TODO: move these tasks to another file
 */
const startupTasks = () => {
  fs.readFile('./graindefs/grainDefs.json', 'utf8', (err, data) => {
    if (err) {
      console.log("Error reading grainDefs.json");
      return;
    }
    grainDefs = JSON.parse(data);
    dimKeys = grainDefs.dimKeys;
    factKeys = grainDefs.factKeys;
  });

  let sqlTasks = [
    'SET search_path TO elt;',
    'CREATE EXTENSION IF NOT EXISTS hstore SCHEMA pg_catalog;'
  ];

  for (let sql of sqlTasks) {
    pgClient.query(sql, (error, data) => {
      if (error) {
        console.log(error);
      }
    });
  }
}

// Error Handler
app.use(errorHandler);

// Establish a connection to postgres
// and fire up the node server
async function connect() {
  try {
    await pgClient.connect((err) => {
      if (err) {
        console.error('pgClient failed to connect', err.stack)
        next(err);
      } else {
        console.log('pgClient connected successfully')
      }
    });
    await sfClient.connect(function(err, conn) {
      if (err) {
        console.error('sfClient failed to connect: ' + err.message);
        next(err);
      } else {
        console.log('sfClient connected successfully');
      }
    });
    await startupTasks();
    app.listen(port);
    console.log(`Express app started on port ${port}`);
    console.log('process.env.DATABASE: ', process.env.DATABASE);
  } catch (err) {
    next(err);
  }
}

connect();

module.exports = { pgClient, sfClient };
