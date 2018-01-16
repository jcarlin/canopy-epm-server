const { dbConnections, dbTypes, getDbConnSettings } = require('./connections.js');
const { 
  querySql,
  updateAppTableSql,
  getDimensionIdSql,
  unnestFactTableKeySql,
  deactivateSql,
  insertSql,
  updateBranch15NatJoinSql,
  updateApp20NatJoinSql,
  updateApp20Sql,
  updateBranch15Sql
} = require('./query.js');

module.exports = { 
  querySql,
  updateAppTableSql,
  getDimensionIdSql,
  unnestFactTableKeySql,
  deactivateSql,
  insertSql,
  updateBranch15NatJoinSql,
  updateApp20NatJoinSql,
  updateApp20Sql,
  updateBranch15Sql,
  dbConnections,
  dbTypes,
  getDbConnSettings
};
