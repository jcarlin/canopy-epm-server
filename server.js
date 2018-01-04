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

const { makeQueryString, makeUpdateQueryString, cxUpsertQueryString } = require('./transforms');
const { makeGrainBlockQueryStrings, makeGrainBrickQueryStrings, makeObjectCodeByTimeView, makeAppNetRevView } = require('./graindefs/query.js');
const { buildTableData } = require('./manifests');
const { stitchDatabaseData, produceVariance } = require('./grid');
const { makeLowerCase } = require('./util');

let grainDefs = {};
let dimKeys = {};
const port = process.env.PORT || 8080;
const app = express();
let dbClient = 'pgClient';
//let dbClient = 'sfClient';

// This will log to console if enabled (npm run-script dev)
debug('booting %o', 'debug');

// Necessary for express to be able
// to read the request body
app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true
  })
);

// PostgreSQL db connection
const pgClient = new pg.Client({
  user: 'canopy_db_admin',
  host: 'canopy-epm-test.cxuldttnrpns.us-east-2.rds.amazonaws.com',
  database: 'canopy_test',
  password: process.env.DB_PASSWORD,
  port: 5432
  // statement_timeout: 120000 // timeout queries after 2 minutes
});

// Snowflake db connection
const sfClient = snowflake.createConnection({
  account: 'ge10380', // 'CANOPYEPM',
  username: 'canopyepm',
  password: process.env.DB_PASSWORD_SF,
  region: 'us-east-1',
  database: 'FIVETRAN',
  schema: 'ELT_ELT',
  warehouse: 'FIVETRAN_WAREHOUSE'
});

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

/**
 * In response to the client app sending
 * a hydrated manifest, send back the completely
 * built table data ready to be consumed by ag-grid.
 */
app.post('/grid', (req, res) => {
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

    const transform = JSON.parse(data);  
    const pinned = manifest.regions[0].pinned;
    const includeVariance = manifest.regions[0].includeVariance;
    const includeVariancePct = manifest.regions[0].includeVariancePct;
    let metrics;
    
    // TODO: remove/improve this hack to handle case differences returned from different dbClients
    if (dbClient === 'sfClient') {
      metrics = transform.metrics.map(metric => `"${metric.toUpperCase()}"`).join(',');
    } else {
      metrics = transform.metrics.map(metric => `"${metric}"`).join(',');
    }
    
    const query = makeQueryString(transform, pinned, dimKeys, metrics);
    debug('GET /grid query: ', query);
    
    // New function to take db data (from pg or sf) and finish remaining work + return
    const stitchData = dbData => {
      debug('data: ', dbData);
      const producedData = stitchDatabaseData(manifest, tableData, dbData);
  
      if (includeVariance && includeVariancePct) {
        const finalData = produceVariance(producedData);
        return res.json(finalData);
      }

      // debug('producedData: ', producedData);
      return res.json(producedData);
    }

    // Handle dbClient type and query db
    if (dbClient === 'pgClient') {

      pgClient.query(query, (error, data) => {
        if (error) {
          debug('pgClient.query error: ', error);
          return res.json({ error });
        }
        
        return stitchData(data.rows);
      });
    } else if (dbClient === 'sfClient') {

      sfClient.execute({
        sqlText: query,
        // binds: [10], // this should work according to docs but is not. Important to avoid sql injection.
        complete: function(error, stmt, data) {
          if (error) {
            console.error('Failed to execute statement due to the following error: ' + error.message);
            return res.json({ error });
          } else {
            // make keys lowercase
            data = data.map(item => {
              for (key in item) {
                item[ key.toLowerCase() ] = item[key];
                delete item[key];
              }
              return item;
            });

            return stitchData(data);  
          }
        }
      });
    }
  });
});

/**
 * Edit a cell by based on an ICE
 */
app.patch('/grid', (req, res) => {
  const ice = req.body.ice;
  const manifest = req.body.manifest;
  let query = '';

  if (!ice || !manifest) {
    return res.status(400).json({
      error: 'You must send data and manifest for independent change event'
    });
  }

  const keys = Object.keys(ice);
  const rowIndex = ice.rowIndex;
  const colIndex = ice.colIndex;
  const newValue = ice.value;
  const region = manifest.regions.find(
    region => region.colIndex === colIndex && region.rowIndex === rowIndex
  );

  fs.readFile(`./transforms/${region.transform}`, (err, data) => {
    if (err) {
      return res.status(400).json({ error: err });
    }

    const transform = JSON.parse(data);
    transform.new_value = newValue;
    const pinned = region.pinned;

    if (transform.isNile) {
      query = cxUpsertQueryString(transform, ice, pinned, dimKeys);
    } else {
      query = makeUpdateQueryString(transform, ice, pinned);
    }

    debug('PATCH /grid query: ', query);

    pgClient.query(query, (error, data) => {
      if (error) {
        return res.status(400).json({ error: 'Error writing to database' });
      }
      return res.json({ data });
    });
  });
});

/**
 * Get an unhydrated manifest from the filesystem.
 * You must specify the type which corresponds to the
 * filename of the manifest on disk.
 */
app.get('/manifest', (req, res) => {
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

// Utility endpoint which you can send
// a manifest to and receive the column and row defs
// produce by running it through `buildTableData`
app.post('/test-manifest', (req, res) => {
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
        return callback("error");
        //return res.status(400).json({ error: 'Error writing to database.' + `${error}` });
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
    app.listen(port);
    console.log(`Express app started on port ${port}`);
    startupTasks();
  } catch (err) {
    console.log(err);
  }
}

connect();
