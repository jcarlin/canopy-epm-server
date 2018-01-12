require('dotenv').config();

const dbConnections = [
  {
    name: 'postgresql',
    settings: {
      user: 'canopy_db_admin',
      host: 'canopy-epm-test.cxuldttnrpns.us-east-2.rds.amazonaws.com',
      database: 'canopy_test',
      password: process.env.DB_PASSWORD_POSTGRESQL,
      port: 5432
    }
  },
  {
    name: 'snowflake',
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

const getDbConnSettings = dbName => {
  return dbConnections.find(conn => {
    return conn.name === dbName;
  });
};

module.exports = { dbConnections, getDbConnSettings };
