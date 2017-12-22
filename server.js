require('dotenv').config();
const fs = require('fs');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');
const pg = require('pg');
const debug = require('debug')('log');
const async = require('async');

const { makeQueryString, makeUpdateQueryString, makeGrainDimQueryString, makeGrainQueryStrings } = require('./transforms');
const { buildTableData } = require('./manifests');
const { stitchDatabaseData, produceVariance } = require('./grid');
const { makeLowerCase } = require('./util');

const app = express();

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

const client = new pg.Client({
  user: 'canopy_db_admin',
  host: 'canopy-epm-test.cxuldttnrpns.us-east-2.rds.amazonaws.com',
  database: 'canopy_test',
  password: process.env.DB_PASSWORD,
  port: 5432
});

const port = process.env.PORT || 8080;

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

// In response to the client app sending
// a hydrated manifest, send back the completely
// built table data ready to be consumed by ag-grid
app.post('/grid', (req, res) => {
  if (!req.body.manifest) {
    return res.status(400).json({
      error:
        'You must supply a manifest. Send it on an object with a `manfifest` key: { manifest: ... }'
    });
  }

  const manifest = req.body.manifest;
  const tableData = buildTableData(manifest); // manifest -> something ag-grid can use

  fs.readFile(`./transforms/${tableData.transforms[0]}`, (err, data) => {
    if (err) {
      return res.json({ err });
    }

    debug("data: " + JSON.parse(data));
    const pinned = manifest.regions[0].pinned;
    const query = makeQueryString(JSON.parse(data), pinned);
    const includeVariance = manifest.regions[0].includeVariance;
    const includeVariancePct = manifest.regions[0].includeVariancePct;

    client.query(query, (error, data) => {
      if (error) {
        return res.json({ error });
      }

      const producedData = stitchDatabaseData(manifest, tableData, data);

      if (includeVariance && includeVariancePct) {
        const finalData = produceVariance(producedData);
        return res.json(finalData);
      }
      return res.json(producedData);
    });
  });
});

app.get('/graintest', (req, res) => {
  debug('/graintest');
  // Get the grainDefs.json file
  fs.readFile(`./graindefs/grainDefsNew.json`, (err, data) => {
    if (err) {
      return res.json({ err });
    }

    const grainDefs = JSON.parse(data).grainDefs;
    const dimKeys = JSON.parse(data).dimKeys;
    let allQueryStrings = "";

    debug("grainDefs", grainDefs);

    grainDefs.map(grainDef => {
      const grainTableName = `grain_${grainDef.id}`;
      const grainSerName = `br${grainDef.id}_oid`;
      const grainDefName = grainDef.name;

      grainDef.memberSet.map(members => {
        let queryStrings = makeGrainQueryStrings({
          members: `'${member.members}'`,
          grainTableName: grainTableName,
          grainSerName: grainSerName,
          dimNumber: dimNumber,
          grainDefName: grainDefName
        });
        allQueryStrings.concat(queryStrings);
      })
    })
  })
});

app.get('/grain', (req, res) => {
  debug('/grain');
  // Get the grainDefs.json file
  fs.readFile(`./graindefs/grainDefsNew.json`, (err, data) => {
    if (err) {
      return res.json({ err });
    }

    const grainDefs = JSON.parse(data).grainDefs;
    const dimKeys = JSON.parse(data).dimKeys;
    let allQueryStrings = "";

    debug("grainDefs", grainDefs);

    /* async.waterfall([
      *     myFirstFunction,
      *     mySecondFunction,
      *     myLastFunction,
      * ], function (err, result) {
      *     // result now equals 'done'
      * });
      * function myFirstFunction(callback) {
      *     callback(null, 'one', 'two');
      * }
      * function mySecondFunction(arg1, arg2, callback) {
      *     // arg1 now equals 'one' and arg2 now equals 'two'
      *     callback(null, 'three');
      * }
      * function myLastFunction(arg1, callback) {
      *     // arg1 now equals 'three'
      *     callback(null, 'done');
      * }*/
    
    async.waterfall([
      function(callback) {
        let obj = [];
        grainDefs.map(grainDef => {
          grainDef.map(member => {
            member.grainTableName = `grain_${grainDef.id}`;
            member.grainSerName = `br${grainDef.id}_oid`;
            member.grainDefName = grainDef.name;
          });
        });

        callback(null, grainDefs);
      },
      function(gd, callback) {
        debug("gd: ", gd);
        callback(null, gd);
      }
      //execGrainSql
    ], function (err, result) {
      debug("waterfall result: ", result);
      debug("err: ", err);
      return res.json({});
    });

    function mapGrainDefs(cb) {
      params = [1, 2, 3];
      cb(null, params)
    }

    function mapMemberSets(params, cb) {
      params.push(4);

      function m(callback) {
        params.map(param => {
          param * 2
        });
      }
      cb(null, params.map(param => {
        param * 2
      }));
      //cb(null, params);
    }

    /*function mapGrainDefs(cb) {
      let params = [];
      async.map(grainDefs, function(grainDef, callback) {
        params.memberSet = grainDef.memberSet;
        params.grainTableName = `grain_${grainDef.id}`;
        params.grainSerName = `br${grainDef.id}_oid`;
        params.grainDefName = grainDef.name;
        
        if (err) return callback(err);
        callback(null, params);
      }, function(err, results) {
        //debug("mapGrainDefs results: ", results);

        if (err) return cb(err);
        cb(null, results);
      });
    }

    function mapMemberSets(params, cb) {
      let allQueryStrings = "";
      debug("params: ", params);
      const memberSet = params.memberSet;

      // Cycle through each memberSet for a grainDef
      async.map(memberSet, function(member, callback) {
        debug("here");
        let queryStrings = makeGrainQueryStrings({
          members: `'${member.members}'`,
          grainTableName: params.grainTableName,
          grainSerName: params.grainSerName,
          dimNumber: dimKeys[member.dimension],
          grainDefName: params.grainDefName
        });

        allQueryStrings.concat(queryStrings);
        debug("allQueryStrings: ", allQueryStrings);
        if (err) return callback(err);
        callback(null, allQueryStrings);
      }, function(err, results) {
        debug("async.map memberset results: ", results);

        if (err) return cb(err);
        cb(null, results);
      });
    }*/

    function execGrainSql(allQueryStrings, callback) {
      debug("allQueryStrings: ", allQueryStrings);
      callback(null, allQueryStrings);
    }
  });
});

// Edit a cell by based on an ICE
app.patch('/grid', (req, res) => {
  const ice = req.body.ice;
  const manifest = req.body.manifest;

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

    const query = makeUpdateQueryString(transform, ice, pinned);

    client.query(query, (error, data) => {
      if (error) {
        return res.status(400).json({ error: 'Error writing to database' });
      }
      return res.json({ data });
    });
  });
});

// Get an unhydrated manifest from the filesystem.
// You must specify the type which corresponds to the
// filename of the manifest on disk
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

// Establish a connection to postgres
// and fire up the node server
async function connect() {
  try {
    await client.connect();
    app.listen(port);
    console.log(`Express app started on port ${port}`);
  } catch (err) {
    console.log(err);
  }
}

connect();
