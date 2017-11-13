require('dotenv').config();
const fs = require('fs');
const express = require('express');
const cors = require('cors');
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

app.use(cors());
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

function listen() {
  if (app.get('env') === 'test') return;
  app.listen(port);
  console.log(`Express app started on port ${port}`);
}

async function connect() {
  try {
    await client.connect();
    listen();
  } catch (err) {
    console.log(err);
  }
}

connect();
