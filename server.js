require('dotenv').config();
const fs = require('fs');
const express = require('express');
var bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');
const safeEval = require('safe-eval');
const pg = require('pg');
const { makeQuery } = require('./transforms');
const { makeLowerCase } = require('./util');

const { buildTableData, extractKeySet } = require('./manifests');

const app = express();

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
  if (!req.body.manifest) {
    return res.status(400).json({ error: 'You must supply a manifest' });
  }

  const tableData = buildTableData(req.body.manifest);

  fs.readFile(`./transforms/${tableData.transforms[0]}`, (err, data) => {
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

      tableData.rowDefs.forEach(def => {
        const keys = Object.keys(def);

        keys.forEach(key => {
          if (typeof def[key] === 'object') {
            const columnKeys = extractKeySet(def[key].columnKey);
            const rowKeys = extractKeySet(def[key].rowKey);

            let rowKeyStrings = rowKeys.map(key => {
              return `row.${key.dimension} === '${key.member}'`;
            });

            let columnKeyStrings = columnKeys.map(key => {
              return `row.${key.dimension} === "${key.member}"`;
            });

            const joinedColumnKeys = columnKeyStrings.join(' && ');
            const joinedRowKeys = rowKeyStrings.join(' && ');
            const totalMatchString = `${joinedColumnKeys} && ${joinedRowKeys}`;

            def[key].value = dbData.rows.find(row => {
              return eval(totalMatchString);
            })['Net Profit'];
          }
        });
      });
      return res.json(tableData);
      // return res.json(dbData.rows);
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

app.post('/manifest', (req, res) => {
  const { manifest } = req.body;

  if (!req.body.manifest) {
    return res.status(400).json({ error: 'You must supply a manifest' });
  }

  const tableData = buildTableData(req.body.manifest);
  return res.json(tableData);
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
