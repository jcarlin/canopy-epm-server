require('dotenv').config();

const dbTypes = Object.freeze({
  "POSTGRESQL": 1, 
  "SNOWFLAKE": 2
});

const dbConnections = [
  {
    type: dbTypes.POSTGRESQL,
    settings: {
      user: 'canopy_db_admin',
      host: 'canopy-epm-test.cxuldttnrpns.us-east-2.rds.amazonaws.com',
      database: 'canopy_test',
      password: process.env.DB_PASSWORD_POSTGRESQL,
      port: 5432
    }
  },
  {
    type: dbTypes.SNOWFLAKE,
    settings: {
      account: 'ge10380', // 'CANOPYEPM',
      username: 'canopyepm',
      password: process.env.DB_PASSWORD_SNOWFLAKE,
      region: 'us-east-1',
      database: 'FIVETRAN',
      schema: 'ELT_ELT',
      warehouse: 'FIVETRAN_WAREHOUSE'
    }
  }
];

// Return a database's connection settings
const getDbConnSettings = (databaseType) => {
  return dbConnections.find(conn => {
    return conn.type === databaseType;
  }).settings;
};

module.exports = { dbConnections, dbTypes, getDbConnSettings };
