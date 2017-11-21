require('dotenv').config();
const fs = require('fs');
const express = require('express');
const cors = require('cors');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');
const safeEval = require('safe-eval');
const pg = require('pg');
const { makeQuery } = require('./transforms');
const { makeLowerCase } = require('./util');

const { buildTableData } = require('./manifests');

const app = express();
const client = new pg.Client({
  user: 'canopy_db_admin',
  host: 'canopy-epm-test.cxuldttnrpns.us-east-2.rds.amazonaws.com',
  database: 'canopy_test',
  password: process.env.DB_PASSWORD,
  port: 5432,
  idleTimeoutMillis: 600000,
  connectionTimeoutMillis: 2000
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

app.use(cors());
// app.use(checkJwt);

app.post('/ping', async (req, res) => {
  fs.readFile('./transforms/sales-by-product.json', (err, data) => {
    if (err) {
      return res.json({ error });
    }

    const query = makeQuery(data);

    client.query(query, (error, data) => {
      if (error) {
        return res.json({ error });
      }

      const dbData = data;

      const findRow = (data, compareString, key) => {
        return data.rows.find(row => {
          return eval(`${compareString} && row.product === key`);
        });
      };

      const getCompareString = (def, key) => {
        return `row.${makeLowerCase(key)} === '${def[key]}'`;
      };

      fs.readFile('./manifests/sales-by-product.json', (err, data) => {
        if (err) {
          return res.json(err);
        }
        const tableData = buildTableData(JSON.parse(data));
        tableData.rowDefs.forEach(def => {
          const keys = Object.keys(def);
          keys.forEach(key => {
            if (typeof def[key] !== 'object' && key !== 'field') {
              def.compareString = getCompareString(def, key);
            }
          });
        });
        tableData.rowDefs.forEach(def => {
          const keys = Object.keys(def);
          keys.forEach(key => {
            const dbRow = findRow(dbData, def.compareString, key);
            if (dbRow && dbRow['Net Profit']) {
              def[key].value = dbRow['Net Profit'];
            } else {
              def[key].value = null;
            }
          });
        });
        return res.json(tableData);
        // return res.json(dbData.rows);
      });
    });
  });
});

app.get('/pong', async (req, res) => {
  fs.readFile('./transforms/periodic-vs-ytd-periodic.json', (err, data) => {
    if (err) {
      return res.json({ error });
    }

    const query = makeQuery(data);

    client.query(query, (error, data) => {
      if (error) {
        return res.json({ error });
      }
      res.json({ data: data.rows });
    });
  });
});

app.get('/manifest', (req, res) => {
  fs.readFile('./manifests/manifest.json', (err, data) => {
    if (err) {
      return res.json({ error: err });
    }
    const manifest = JSON.parse(data);
    res.json({ manifest });
  });
});

app.get('/data', (req, res) => {
  fs.readFile('./transforms/sales-by-product.json', (err, data) => {
    if (err) {
      return res.json({ error });
    }

    const query = makeQuery(data);

    client.query(query, (error, data) => {
      if (error) {
        return res.json({ error });
      }
      res.json({ data });
    });
  });
});

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
