require('dotenv').config();
const fs = require('fs');
const express = require('express');
const cors = require('cors');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');
const { Client } = require('pg');
const { makeQuery } = require('./transforms');

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
app.use(checkJwt);
app.get('/ping', async (req, res) => {
  fs.readFile('./transforms/sales-by-product.json', (err, data) => {
    if (err) {
      res.json({ error });
    }
    const query = makeQuery(data);
    client.query(query, (error, data) => {
      if (error) {
        return res.json({ error });
      }
      return res.json({ result: data.rows });
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
