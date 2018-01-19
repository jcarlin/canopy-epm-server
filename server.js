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

let grainDefs = {};
let dimKeys = {};
let factKeys = [];

// This will log to console if enabled (npm run-script dev)
debug('booting %o', 'debug');

// Necessary for express to be able to read the request body
app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true
  })
);

// Define database clients/connections
const pgClient = new pg.Client(database.getDbConnSettings(database.dbTypes.POSTGRESQL));
const sfClient = snowflake.createConnection(database.getDbConnSettings(database.dbTypes.SNOWFLAKE));

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

app.get('/database', (req, res) => {
  console.log('GET /database process.env.DATABASE: ', process.env.DATABASE);
  const dbMap = database.dbConnections.map(conn => {
    conn.active = (conn.type == process.env.DATABASE);
    return conn;
  });
  
  return res.json(dbMap);
});

app.post('/database', (req, res) => {
  debug('POST /database');
  console.log('POST /database process.env.DATABASE BEFORE: ', process.env.DATABASE);
  console.log('POST /database req.body.database: ', req.body.database);
  
  if (!req.body.database || !database.dbTypes[req.body.database.toUpperCase()]) {
    return res.status(400).json({
      error: 'You must supply a database. Send it on an object with a `database` key: { database: ... }'
    });
  }

  process.env.DATABASE = database.dbTypes[req.body.database.toUpperCase()];
  console.log('POST /database process.env.DATABASE AFTER: ', process.env.DATABASE);

  const dbMap = database.dbConnections.map(conn => {
    conn.active = (conn.type == process.env.DATABASE);
    return conn;
  });

  return res.json(dbMap);
});

/**
 * In response to the client app sending
 * a hydrated manifest, send back the completely
 * built table data ready to be consumed by ag-grid.
 */
app.post('/grid', (req, res) => {
  debug('POST /grid');
  if (!req.body.manifest) {
    return res.status(400).json({
      error:
        'You must supply a manifest. Send it on an object with a `manfifest` key: { manifest: ... }'
    });
  }

  const manifest = req.body.manifest;
  const tableData = buildTableData(manifest); // manifest -> something ag-grid can use

  fs.readFile(`./transforms/${tableData.transforms[0]}`, 'utf8', (err, data) => {
    if (err) {
      return res.json({ err });
    }

    let sql = null;
    const transform = JSON.parse(data);
    const dimensions = transform.dimensions;
    const pinned = getPinnedSet(manifest.regions[0].pinned);
    const includeVariance = manifest.regions[0].includeVariance;
    const includeVariancePct = manifest.regions[0].includeVariancePct;
    
    // Format response object
    const stitchData = dbData => {
      if (process.env.DATABASE == database.dbTypes.SNOWFLAKE) {
        console.log("stitchData for snowflake");
        dbData = dbData.map(row => {
          return _.transform(row, function (result, val, key) {
            result[key.toLowerCase()] = val;
          });
        });
        const producedData = stitchDatabaseData(manifest, tableData, dbData);

        if (includeVariance && includeVariancePct) {
          const finalData = produceVariance(producedData); 
          res.json(finalData);
        }

        res.json(producedData);
      } else {
        const producedData = stitchDatabaseData(manifest, tableData, dbData);

        if (includeVariance && includeVariancePct) {
          const finalData = produceVariance(producedData); 
          res.json(finalData);
        }

        res.json(producedData);
      }
    };

    // Legacy (app table) query
    if (!transform.isNile) {
      const sqlParams = {
        dimensions: dimensions.join(','),
        metrics: transform.metrics.map(metric => `"${metric}"`).join(','),
        tableName: transform.table,
        filterStatements: pinned.map(filter => {
          return `${filter.dimension} IN ('${filter.member}')`;
        }).join(' AND ')
      }
      if (process.env.DATABASE == database.dbTypes.SNOWFLAKE) {
        sqlParams.filterStatements = pinned.map(filter => {
          return `${capitalize(filter.dimension)} IN ('${filter.member}')`;
        }).join(' AND ');
        sqlParams.metrics = `"NET_PROFIT"`; // HACK!
        // querySql
        sql = database.querySql(sqlParams);

        // Execute querySql
        sfClient.execute({
          sqlText: sql,
          complete: function(error, stmt, data) {
            if (error) {
              return res.json({ error });
            }

            stitchData(data);
          } // querySql
        });
      } else { // POSTGRESQL
        sql = database.querySql(sqlParams);

        pgClient.query(sql, (error, data) => {
          if (error) {
            return res.status(400).json({ error: 'Error writing to database' });
          }
  
          stitchData(data.rows);
        });
      }
    } else {
      const sqlParams = {
        dimensions: dimensions,
        metrics: null,
        tableName: transform.table,
        filterStatements: null
      };
  
      const dimensionsWithKeys = mergeDimKeys(pinned, dimKeys);
      
      // getDimensionIdSql
      sql = database.getDimensionIdSql(dimensionsWithKeys);
      
      // Why is this converting to string??
      const db = Number(process.env.DATABASE);
      switch(db) {
        case database.dbTypes.SNOWFLAKE:
          
          // Set params according to db nuances
          sqlParams.metrics = transform.metrics.map(metric => `"${metric.toUpperCase().split(' ').join('_')}"`).join(',');
          sqlParams.table = transform.table.split(' ').join('_');
          
          // Execute getDimensionIdSql
          sfClient.execute({
            sqlText: sql,
            complete: function(err, stmt, data) {
              if (err) {
                return res.status(400).json({ err: 'Error writing to database' + err.message });
              }
              
              // Merge dimension values into dimension array
              const dimValuesObj = data[0];
              const dimensionsWithVals = mergeDimVals(dimensionsWithKeys, dimValuesObj);
              
              sqlParams.filterStatements = dimensionsWithVals.map(dim => {
                return dim.idWhereClause;
              }).join(' AND ');
    
              // querySql
              sql = database.querySql(sqlParams);          
              sfClient.execute({
                sqlText: sql,
                complete: function(error, stmt, data) {
                  if (error) {
                    return res.json({ error });
                  }

                  stitchData(data);
                } // querySql
              });
            } // getDimensionIdSql
          });
          break;
        case database.dbTypes.POSTGRESQL:
          // Set params according to db nuances
          sqlParams.metrics = transform.metrics.map(metric => `"${metric}"`).join(',');
          
          // Execute getDimensionIdSql
          pgClient.query(sql, (error, data) => {
            if (error) {
              res.status(400).json({ error: 'Error writing to database' });
            }
            // Merge dimension values into dimension array
            const dimValuesObj = data.rows[0];
            const dimensionsWithVals = mergeDimVals(dimensionsWithKeys, dimValuesObj);
            
            sqlParams.filterStatements = dimensionsWithVals.map(dim => {
              return dim.idWhereClause;
            }).join(' AND ');
  
            // querySql
            sql = database.querySql(sqlParams);          
            pgClient.query(sql, (error, data) => {
              if (error) {
                return res.json({ error });
              }
              stitchData(data.rows);
            }); // querySql
          }); // getDimensionIdSql
          break;
      }
    } // manifest.isNile
  });
});

/**
 * Edit a cell by based on an ICE
 */
app.patch('/grid', (req, res) => {
  if (!req.body.ice || !req.body.manifest) {
    return res.status(400).json({
      error: 'You must send data and manifest for independent change event'
    });
  }

  let sql = null;
  const ice = req.body.ice;
  const manifest = req.body.manifest;
  const rowIndex = ice.rowIndex;
  const colIndex = ice.colIndex;
  const newValue = ice.value;
  const region = manifest.regions.find(region => {
    return region.colIndex === colIndex && region.rowIndex === rowIndex;
  });

  fs.readFile(`./transforms/${region.transform}`, (err, data) => {
    if (err) {
      return res.status(400).json({ error: err });
    }
    
    const transform = JSON.parse(data);
    const keySets = buildKeySet(extractKeySetAndId(ice.rowKey), extractKeySetAndId(ice.columnKey), getPinnedSet(region.pinned));
    
    const sqlParams = {
      tableName: null,
      factId: null,
      metric: transform.metrics[0],
      newValue: ice.value,
      filterStatements: null,
      dimIdValues: null,
      dimIdColumns: null
    };
    
    // Legacy (app table) update
    if (!transform.isNile) {
      
      sqlParams.tableName = transform.table;
      sqlParams.filterStatements = keySets.map(key => {
        return `${key.dimension} = '${key.member}'`;
      }).join(' AND ');
      
      // updateAppTableSql
      sql = database.updateAppTableSql(sqlParams);
      
      // Why is this converting to string??
      const db = Number(process.env.DATABASE);
      switch(db) {
        case database.dbTypes.POSTGRESQL:
          pgClient.query(sql, (error, data) => {
            if (error) {
              return res.status(400).json({ error: 'Error writing to database' });
            }

            res.json({ data });
          }); // updateAppTableSql
          break;
        case database.dbTypes.SNOWFLAKE:
          sqlParams.table = transform.table.split(' ').join('_');
         
          sfClient.execute({
            sqlText: sql,
            complete: function(error, stmt, data) {
              if (error) {
                return res.status(400).json({ error: 'Error writing to database' });
              }

              res.json({ data });
            }
          }); // updateAppTableSql
          break;
      }
    } 
    else {
      // const dimensionsWithKeys = mergeDimKeys(keySets, dimKeys);
      // sql = database.getDimensionIdSql(dimensionsWithKeys);

      const factInfo = mergeFactKeys(transform.metrics, factKeys)[0];
      sqlParams.factId = factInfo.fact_id;
      sqlParams.tableName = `root_${factInfo.fact_id}`;
      let dimensions = mergeDimKeys(keySets, dimKeys);
      
      // getDimensionIdSql
      sql = database.getDimensionIdSql(dimensions);

      // Why is this converting to string??
      const db = Number(process.env.DATABASE);
      switch(db) {
        case database.dbTypes.POSTGRESQL:
          // Execute getDimensionIdSql
          pgClient.query(sql, (error, data) => {
            if (error) {
              return res.status(400).json({ error: 'Error writing to database' });
            }
            
            // Merge dimension values into dimension array
            const dimValuesObj = data.rows[0];
            const dimensionsWithVals = mergeDimVals(dimensions, dimValuesObj);
            sqlParams.filterStatements = dimensionsWithVals.map(dim => {
              return dim.idWhereClause;
            }).join(' AND ');

            // deactivateSql
            sql = database.deactivateSql(sqlParams);

            // Execute deactivateSql
            pgClient.query(sql, (error, data) => {
              if (error) {
                return res.status(400).json({ error: 'Error writing to database' });
              }
              // Set additional sqlParams values
              sqlParams.dimIdValues = dimensionsWithVals.map(dim => { return dim.value; });
              sqlParams.dimIdColumns = dimensionsWithVals.map(dim => { return dim.idColName; });
              
              // Make insertSql
              sql = database.insertSql(sqlParams);

              // Execute insertSql
              pgClient.query(sql, (error, data) => {
                if (error) {
                  return res.status(400).json({ error: 'Error writing to database' });
                }
                
                // updateBranch15NaturalJoinSql
                sql = database.updateBranch15NatJoinSql(sqlParams);
                
                // Execute updateBranch15NaturalJoinSql
                pgClient.query(sql, (error, data) => {
                  if (error) {
                    return res.status(400).json({ error: 'Error writing to database' });
                  }
                
                  // updateApp20NatJoinSql
                  sql = database.updateApp20NatJoinSql(sqlParams);
                  
                  // Execute updateApp20NatJoinSql
                  pgClient.query(sql, (error, data) => {
                    if (error) {
                      return res.status(400).json({ error: 'Error writing to database' });
                    }
                    res.json({ data });
                  }); //updateApp20NatJoinSql
                }); // updateBranch15NaturalJoinSql
              }); // insertSql
            }); // deactivateSql
          }); // getDimensionIdSql
          break;
        case database.dbTypes.SNOWFLAKE:
          // Execute getDimensionIdSql
          sfClient.execute({
            sqlText: sql,
            complete: function(error, stmt, data) {
              if (error) {
                return res.status(400).json({ error: 'Error writing to database' });
              }
              // Merge dimension values into dimension array
              const dimValuesObj = data[0];
              dimensions = mergeDimVals(dimensions, dimValuesObj);
              sqlParams.filterStatements = dimensions.map(dim => {
                return dim.idWhereClause;
              }).join(' AND ');

              // deactivateSql
              sql = database.deactivateSql(sqlParams);

              // Execute deactivateSql
              sfClient.execute({
                sqlText: sql,
                complete: function(error, stmt, data) {
                  if (error) {
                    return res.status(400).json({ error: 'Error writing to database' });  
                  }
                  // Set additional sqlParams values
                  sqlParams.dimIdValues = dimensions.map(dim => { return dim.value; });
                  sqlParams.dimIdColumns = dimensions.map(dim => { return dim.idColName; });
                  
                  // Make insertSql
                  sql = database.insertSql(sqlParams);
                      
                  // Execute insertSql
                  sfClient.execute({
                    sqlText: sql,
                    complete: function(error, stmt, data) {
                      if (error) {
                        return res.status(400).json({ error });  
                      }
                      // updateBranch15Sql
                      sql = database.updateBranch15Sql(sqlParams);
                      
                      // Execute updateBranch15Sql
                      sfClient.execute({
                        sqlText: sql,
                        complete: function(error, stmt, data) {
                          if (error) {
                            console.error('Failed to execute statement due to the following error: ' + error.message);
                            return res.status(400).json({ error: 'Error writing to database' });
                          } 
                          // updateApp20Sql
                          sql = database.updateApp20Sql(sqlParams);
                          
                          // Execute updateApp20Sql
                          sfClient.execute({
                            sqlText: sql,
                            complete: function(error, stmt, data) {
                              if (error) {
                                return res.status(400).json({ error: 'Error writing to database' });
                              }
                              res.json({ data });
                            } // updateApp20Sql
                          });
                        } // updateBranch15Sql
                      });
                    } // insertSql
                  });
                } // deactivateSql
              });
            } // getDimensionIdSql
          });
          break;
      } // switch(database)
    } // !transform.isNile
  });
});

/**
 * Get an unhydrated manifest from the filesystem.
 * You must specify the type which corresponds to the
 * filename of the manifest on disk.
 */
app.get('/manifest', (req, res) => {
  debug('GET /manifest');
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
});

app.get('/statistics', (req, res) => {
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
});

// Utility endpoint which you can send
// a manifest to and receive the column and row defs
// produce by running it through `buildTableData`
app.post('/test-manifest', (req, res) => {
  debug('/test-manifest');
  const { manifest } = req.body;

  if (!req.body.manifest) {
    return res.status(400).json({
      error:
        'You must supply a manifest. Send it on an object with a `manfifest` key: { manifest: ... }'
    });
  }

  const tableData = buildTableData(req.body.manifest);
  return res.json(tableData);
});

// Utility end point to output sql string of all graindef scripts
app.get('/grain', (req, res) => {
  try {
    return res.json({grainDefs});
  } catch (err) {
    return res.json({err});
  }
});

/**
 * System admin utility route to handle creation of grain tables
 * 
 * 2018-01-03 run time: 5m7s
 */
app.post('/grain', (req, res) => {
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
      
      allQueryStrings = `${allQueryStrings}${queryStrings}${makeAppNetRevView()}${makeObjectCodeByTimeView()}`;
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

// Establish a connection to postgres
// and fire up the node server
async function connect() {
  try {
    await pgClient.connect((err) => {
      if (err) {
        console.error('pgClient failed to connect', err.stack)
      } else {
        console.log('pgClient connected successfully')
      }
    });
    await sfClient.connect(function(err, conn) {
      if (err) {
        console.error('sfClient failed to connect: ' + err.message);
      } else {
        console.log('sfClient connected successfully');
      }
    });
    await startupTasks();
    app.listen(port);
    console.log(`Express app started on port ${port}`);
    console.log('process.env.DATABASE: ', process.env.DATABASE);
  } catch (err) {
    console.log(err);
  }
}

connect();

module.exports = { pgClient, sfClient };
