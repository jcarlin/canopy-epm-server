const { Pool } = require('pg');
const snowflake = require('snowflake-sdk');

process.env.DB_TYPES = ['POSTGRESQL', 'SNOWFLAKE'];

/** 
 * POSTGRESQL
*/
// Alternative connection with URI
// const connectionString = `postgresql://${process.env.PGUSER}:${process.env.PGPASSWORD}@${process.env.PGHOST}:${process.env.PGPORT}/${PGDATABASE}`;
// const pool = new Pool({
//   connectionString: connectionString,
// })

// Use .env variables for connection (i.e. PGHOST, PGUSER, etc)
const pool = new Pool();

const startupSql = `SET search_path TO elt; CREATE EXTENSION IF NOT EXISTS hstore SCHEMA pg_catalog;`;

pool.query(startupSql, (err, res) => {
  if (err) {
    console.log('PostgreSQL failed. startupSql errored.')
    return process.exit(-1) // Shutdown node
  }
  console.log('PostgreSQL successfully running and `startupSql` executed.')
})

pool.on('error', (err, client) => {
  console.error('Unexpected error on idle client', err)
  process.exit(-1)
})

/** 
 * SNOWFLAKE
*/
const sfClient = snowflake.createConnection({
  account:  process.env.SFACCOUNT     || 'ge10380', 
  username: process.env.SFUSER        || 'canopyepm',
  password: process.env.SFPASSWORD,
  region:   process.env.SFREGION      || 'us-east-1',
  database: process.env.SFDATABASE    || 'FIVETRAN',
  schema:   process.env.SFSCHEMA      || 'ELT_ELT',
  warehouse: process.env.SFWAREHOUSE  || 'FIVETRAN_WAREHOUSE'
});

sfClient.connect(function(err, conn) {
  if (err) {
    console.error('Unable to connect to Snowflake: ' + err.message);
    return process.exit(-1); // Shutdown node
  }
  console.log('Snowflake successfully connected as id: ' + sfClient.getId());
});

/**
 * All queries route through here 
 */
const query = (sql, params) => {
  return new Promise((resolve, reject) => {
    console.log(sql);

    // TODO
    if (process.env.DB_TYPE === 'POSTGRESQL') { //postgres
      console.time('pgQuery')
      pool.query(sql, params)
      .then(res => {
        console.timeEnd('pgQuery')
        resolve(res.rows)
      })
      .catch(err => {
        console.timeEnd('pgQuery')
        reject(err)
      })
    }

    if (process.env.DB_TYPE === 'SNOWFLAKE') { //snowflake
      console.time('sfQuery')
      sfClient.execute({
        sqlText: sql,
        complete: function(err, stmt, data) {
          console.timeEnd('sfQuery')
          if (err) return reject(err);
          return resolve(data);
        }
      });
    }
  })
}

module.exports = { query }
