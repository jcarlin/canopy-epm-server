const Router = require('express-promise-router')
const db = require('../db')

const dbConnections = (activeDb) => {
  return [
    {name: 'POSTGRESQL', active: activeDb === 'POSTGRESQL' },
    {name: 'SNOWFLAKE', active: activeDb === 'SNOWFLAKE' }
  ];
};

// create a new express-promise-router
// this has the same API as the normal express router except
// it allows you to use async functions as route handlers
const router = new Router()

router.get('/', async (req, res, next) => {
  if (!process.env.DB_TYPE) {
    return res.status(500).json({
      error: 'DB_TYPES has not been set by the server.'
    });
  }

  const conns = dbConnections(process.env.DB_TYPE)
  return res.json(conns);
});

router.post('/', async (req, res, next) => {
  const newDatabase = req.body.database;
  if (!newDatabase) {
    return res.status(400).json({
      error: `You must supply a database. 
        Send it on an object with a 'database' key: { database: ... }`
    });
  }
  
  const match = dbConnections(process.env.DB_TYPE).find(conn => {
    return conn.name === newDatabase;
  });

  if (!match) {
    return res.status(400).json({
      error: `Database must be one of the following: ${process.env.DB_TYPES}`
    }); 
  }
  
  process.env.DB_TYPE = newDatabase;
 
  console.log('Database switched to: ', newDatabase);
  //return res(`Database switched to: ${newDatabase}`);
  return res.json(dbConnections(process.env.DB_TYPE));
});

// export our router to be mounted by the parent application
module.exports = router
