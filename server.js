require('dotenv').config();
const fs = require('fs');
const express = require('express');
const cors = require('cors');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');
const safeEval = require('safe-eval');
const { Client } = require('pg');
const { makeQuery } = require('./transforms');
const { seekElements, getExtractedElements } = require('./util');
const { uniqBy } = require('lodash');

const app = express();
const client = new Client({
  user: 'ryanchenkie',
  host: 'localhost',
  database: 'template1',
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

      fs.readFile('./manifests/manifest.json', (err, data) => {
        const manifest = JSON.parse(data);
        let extractedColumns = getExtractedElements(manifest, 'columns');
        let extractedRows = getExtractedElements(manifest, 'rows');

        const interestedColumns = extractedColumns[0];
        const interestedRows = extractedRows[0];

        let rowDefs = [];
        let colDefs = [];
        interestedRows.rows.forEach(row => {
          let rowKeys = Object.keys(row);
          let rootRow = {
            ...row
          };

          interestedColumns.columns.forEach(column => {
            let rootColumn = {
              ...column
            };
            const columnKeys = Object.keys(column);
            const rowKeys = Object.keys(row);
            let keyString = '';
            let compareString = '';
            columnKeys.forEach((key, i) => {
              if (key !== 'editable')
                i === columnKeys.length - 1
                  ? (keyString += `${column[key].value}`)
                  : (keyString += `${column[key].value}_`);
              compareString += `dbRow.${key} === column.${key}.value && `;
            });
            rowKeys.forEach((key, i) => {
              if (key !== 'editable') {
                compareString += `dbRow.${key} === row.${key}`;
                i === rowKeys.length - 1
                  ? (compareString += '')
                  : (compareString += ' && ');
              }
            });

            rootColumn['field'] = keyString;

            rootRow[keyString] = {
              value: dbData.rows.find(dbRow => eval(compareString))[
                interestedRows['metric']
              ],
              editable: !!column.editable && !!row.editable
            };

            let emptyKeys = columnKeys
              .filter(key => key !== 'editable')
              .map((key, i) => {
                return '';
              });

            let finalRowKeys = rowKeys
              .filter(key => key !== 'editable')
              .map(key => {
                return {
                  ...emptyKeys,
                  field: key
                };
              });

            colDefs.push(...finalRowKeys, rootColumn);
          });
          rowDefs.push(rootRow);
        });

        return res.json({
          colDefs: uniqBy(colDefs, 'field'),
          rowDefs
        });
      });
    });
  });
});

app.get('/pong', async (req, res) => {
  fs.readFile('./transforms/sales-by-product.json', (err, data) => {
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
      return res.json({ error });
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
