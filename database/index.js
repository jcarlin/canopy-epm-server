const { dbConnections, dbTypes, getDbConnSettings, getActiveDb } = require('./connections.js');
const { 
  querySql,
  updateAppTableSql,
  getDimensionIdSql,
  unnestFactTableKeySql,
  deactivateSql,
  insertSql,
  sfInsertSql,
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
  sfInsertSql,
  updateBranch15NatJoinSql,
  updateApp20NatJoinSql,
  updateApp20Sql,
  updateBranch15Sql,
  dbConnections,
  dbTypes,
  getDbConnSettings,
  getActiveDb
};
