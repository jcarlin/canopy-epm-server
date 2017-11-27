require('dotenv').config();
const fs = require('fs');
const express = require('express');
var bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');
const safeEval = require('safe-eval');
const pg = require('pg');
const { makeQuery, makeUpdateQuery } = require('./transforms');
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
  if (!req.body.manifest) {
    return res.status(400).json({
      error:
        'You must supply a manifest. Send it on an object with a `manfifest` key: { manifest: ... }'
    });
  }

  const manifest = req.body.manifest;
  const tableData = buildTableData(manifest);

  // const getData = async transform => {
  //   console.log(transform);
  //   const file = fs.readFileSync(`./transforms/${transform}`);
  //   const query = makeQuery(JSON.parse(file));
  //   return await client.query(query);
  // try {
  //   const file = await Promise.resolve(
  //     fs.readFile(`./transforms/${transform}`)
  //   );
  //   const query = makeQuery(JSON.parse(file));
  //   return await Promise.resolve(client.query(query));
  // } catch (err) {
  //   return err;
  // }
  // };

  // let dbData = [];
  // tableData.transforms.forEach(transform => {
  //   dbData.push({ [transform]: getData(transform) });
  // });
  // console.log(getData(tableData.transforms[0]));

  fs.readFile(`./transforms/${tableData.transforms[0]}`, (err, data) => {
    if (err) {
      return res.json({ error });
    }

    const pinned = manifest.regions[0].pinned;
    const query = makeQuery(JSON.parse(data), pinned);
    const includeVariance = manifest.regions[0].includeVariance;
    const includeVariancePct = manifest.regions[0].includeVariancePct;

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
            const colIndex = def[key].colIndex;
            const rowIndex = def[key].rowIndex;
            const columnKeys = extractKeySet(def[key].columnKey);
            const rowKeys = extractKeySet(def[key].rowKey);
            const pinned = manifest.regions.find(region => {
              return (
                region.colIndex === colIndex && region.rowIndex === rowIndex
              );
            }).pinned;

            let rowKeyStrings = rowKeys.map(key => {
              return `row.${key.dimension} === '${key.member}'`;
            });

            let columnKeyStrings = columnKeys.map(key => {
              return `row.${key.dimension} === "${key.member}"`;
            });

            const joinedColumnKeys = columnKeyStrings.join(' && ');
            const joinedRowKeys = rowKeyStrings.join(' && ');
            const totalMatchString = `${joinedColumnKeys} && ${joinedRowKeys}`;

            const col = tableData.colDefs.find(colDef => {
              if (colDef.hasOwnProperty('properties')) {
                const field = colDef.properties.field;
                return field === key;
              }
            });

            const match = dbData.rows.find(row => {
              return eval(totalMatchString);
            });

            match
              ? (def[key].value = match[pinned[0].member])
              : (def[key].value = null);

            // console.log(def[key].columnKey, col.properties.editable, def[key].editable)

            // const isEditable = !!col.properties.editable && !!def[key].editable;

            // def[key].editable = isEditable;
          }
        });
      });

      if (includeVariance && includeVariancePct) {
        tableData.rowDefs.forEach(def => {
          const keys = Object.keys(def);
          let keyBag = [];
          keys.forEach((key, i) => {
            keyBag.push(key);
            if (/Variance/.test(def[key].columnKey)) {
              def[key].value =
                def[keyBag[i - 1]].value - def[keyBag[i - 2]].value;
            }
            if (/Variance %/.test(def[key].columnKey)) {
              def[key].value =
                def[keyBag[i - 1]].value / def[keyBag[i - 2]].value * 100;
            }
          });
        });
      }
      return res.json(tableData);
    });
  });
});

app.patch('/ping', (req, res) => {
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

    const query = makeUpdateQuery(transform, ice, pinned);

    client.query(query, (error, data) => {
      if (error) {
        return res.status(400).json({ error: 'Error writing to database' });
      }
      return res.json({ data });
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
    return res.status(400).json({
      error:
        'You must supply a manifest. Send it on an object with a `manfifest` key: { manifest: ... }'
    });
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
