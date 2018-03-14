const Router = require('express-promise-router')
const db = require('../db')
const util = require('./../util')
const graindefs = require('./../graindefs')
const router = new Router()

// Helper function to get s_dim info for dimensions passed
const getDimInfo = (member, dimKeys) => {
  return new Promise((resolve, reject) => {
    // Get dimension info
    const dimInfo = dimKeys.find(dimKey => {
      return dimKey.dim_name === member.dimension;
    });

    if (!dimInfo) {
      reject(new Error('Could not find dimInfo for member.'))
    }
    resolve(dimInfo)
  })
}

// Helper function to assemble require sql for graindef creation
const createGrainDefSql = (graindef, member, dimInfo, hierKeys, schema) => {
  return new Promise((resolve, reject) => {
    let queryStrings = "";
    
    // params for the sql generation for this graindef
    const sqlParams = {
      members: `'${member.members}'`,
      grainTableName: `grain_${graindef.id}`,
      graindefName: graindef.name,
      graindefId: graindef.id,
      grainSerName: `gr${graindef.id}_oid`,
      dimNumber: dimInfo.dim_id,
      dimByte: dimInfo.dim_byte === 2 ? 'SMALLINT' : 'INTEGER',
      schema: schema || 'elt' // XXX
    };

    // Get hierarchy info
    if (member.hierarchy) {
      const hierInfo = hierKeys.find(hierKey => {
        return hierKey.hier_name === member.hierarchy;
      });

      if (!hierInfo) {
        return reject(new Error('createGraindefSql could not find hierInfo'));
      }

      sqlParams.hierNumber = hierInfo.hier_id;
      sqlParams.hierName = member.hierarchy;
    }
    
    // Assemble the sql for the brick/block creation
    if (graindef.grainType === "brick" && member.memberSetType === "evaluated") {
      queryStrings = graindefs.makeGrainBrickSql(sqlParams);
    } else if (graindef.grainType === "block") {
      
      // TODO: remove this hack (that grabs the parent graindef (table))
      if (member.platformType === "node_leaf") {
        let parentDimNumber = graindef.id -1;
        member.parentTableName = `grain_${parentDimNumber}`
      }

      // Extract object with database postgres
      if (member.memberSetType === "compute") {
        member.memberSetCode = member.memberSetCode.find(msc => {
          return msc.database === "postgres";
        });
      }
      queryStrings = graindefs.makeGrainBlockSql(sqlParams, {"memberSet": member});
    }

    if (queryStrings.length < 1) {
      return reject(new Error('No queryStrings returned.'))
    }
    return resolve(queryStrings[0]);
  })
}

const createGrainDefSqlSf = (graindef, member, dimInfo, hierKeys, schema) => {
  return new Promise((resolve, reject) => {
    let queryStrings = "";
    
    // params for the sql generation for this graindef
    const sqlParams = {
      members: `'${member.members}'`,
      grainTableName: `grain_${graindef.id}`,
      graindefName: graindef.name,
      graindefId: graindef.id,
      grainSerName: `gr${graindef.id}_oid`,
      dimNumber: dimInfo.dim_id,
      dimByte: dimInfo.dim_byte === 2 ? 'SMALLINT' : 'INTEGER',
      schema: schema || 'elt' // XXX
    };

    // Get hierarchy info
    if (member.hierarchy) {
      const hierInfo = hierKeys.find(hierKey => {
        return hierKey.hier_name === member.hierarchy;
      });

      if (!hierInfo) {
        return reject(new Error('createGraindefSql could not find hierInfo'));
      }

      sqlParams.hierNumber = hierInfo.hier_id;
      sqlParams.hierName = member.hierarchy;
    }
    
    // Assemble the sql for the brick/block creation
    if (graindef.grainType === "brick" && member.memberSetType === "evaluated") {
      queryStrings = graindefs.makeGrainBrickSqlSf(sqlParams);
    } else if (graindef.grainType === "block") {
      
      // TODO: remove this hack (that grabs the parent graindef (table))
      if (member.platformType === "node_leaf") {
        let parentDimNumber = graindef.id -1;
        member.parentTableName = `grain_${parentDimNumber}`
      }

      // Extract object with database postgres
      if (member.memberSetType === "compute") {
        member.memberSetCode = member.memberSetCode.find(msc => {
          return msc.database === "postgres";
        });
      }
      queryStrings = graindefs.makeGrainBlockSqlSf(sqlParams, {"memberSet": member});
    }

    return resolve(queryStrings);
  })
}

// ROUTE: Returns the list of graindefs
router.get('/', async (req, res, next) => {
  results = await db.query("SELECT graindef_graindef FROM model.graindef ORDER BY graindef_id;", null, 'POSTGRESQL');
  results = results.map(row => {
    return row.graindef_graindef[0];
  })
  return res.json(results);
});

// ROUTE: Create single graindef
router.patch('/', async (req, res, next) => {
  let results = null;
  const graindefString = req.body.graindef;
  
  // Check for multiple objects send? Should always be one with current implementation.
  const graindefJson = JSON.parse(graindefString)[0];
  
  // TODO - add support for multiple memberSets objects
  const memberSet = graindefJson.memberSets[0];
  const dimKeys = JSON.parse(process.env.DIM_KEYS);
  const hierKeys = JSON.parse(process.env.HIER_KEYS);
  const dimInfo = await getDimInfo(memberSet, dimKeys)
  
  // Get and execute upsert model.graindef sql
  sqlParams = {
    id: graindefJson.id, 
    name: graindefJson.name, 
    dimension: memberSet.dimension, 
    graindefString: graindefString.replace(/'/g, "''")
  };
  sql = graindefs.upsertGraindefSql(sqlParams);
  results = await db.query(sql, null, 'POSTGRESQL');
  
  /**
   * POSTGRESQL
  */

  // Get and execute graindef creation sql 
  const graindefSql = await createGrainDefSql(graindefJson, memberSet, dimInfo, hierKeys, 'elt');
  results = await db.query(graindefSql, null, 'POSTGRESQL');

  // Gather all the views (that should be recreated), and execute their sql
  const files = await util.readDir('./db/postgresql/views');
  files.map(async (file) => {
    const sql = await util.readFile(`./db/postgresql/views/${file}`);
    results = await db.query(sql, null, 'POSTGRESQL');
  });

  // Async parallel execute sql for all the views (that should be recreated)
  await Promise.all(files).then((sqlResults) => {
    // return res.json(`Successfully executed sql to create graindef ${graindefJson.name}.`);
  });

  /**
   * SNOWFLAKE
  */

  // Drop table 
  let dropTableSql = `DROP TABLE IF EXISTS grain_${graindefJson.id} CASCADE;`;
  await db.query(dropTableSql, null, 'SNOWFLAKE');
  
  // Get and execute graindef creation sql 
  const graindefSqlSf = await createGrainDefSqlSf(graindefJson, memberSet, dimInfo, hierKeys);
  await db.query(graindefSqlSf, null, 'SNOWFLAKE');

  results = await db.query(`SELECT * FROM grain_${graindefJson.id}`, null, 'POSTGRESQL');
  const valueList = results.map(row => {
    return `(${Object.values(row).join(", ")})`;
  }).join(", ");

  await db.query(`INSERT INTO grain_${graindefJson.id} VALUES ${valueList}`, null, 'SNOWFLAKE');
  return res.json(`Successfully executed sql to create graindef ${graindefJson.name}.`);
});

// ROUTE: Old 'generate all graindef' route:
// router.post('/', async (req, res, next) => {
//   const graindefId = req.body.graindefId;
//   const graindefName = req.body.graindefName;
//   const graindefsJson = await util.readJsonFile('./graindefs/graindefs.json')
//   // TODO - remove dimKeys and hierKeys from graindefs.json.
//   const dimKeys = graindefsJson.dimKeys
//   const hierKeys = graindefsJson.hierKeys
//   let allQueryStrings = ''

//   const graindefsPromMap = graindefsJson.graindefs.map(async (graindef) => {
//     const member = graindef.memberSets[0];
//     const dimInfo = await getDimInfo(member, dimKeys)
//     const memberSql = await createGrainDefSql(graindef, member, dimInfo, hierKeys)
//     const results = await db.query(memberSql)
//     return results // Currently this sql returns nothing
//   })

//   await Promise.all(graindefsPromMap).then((results) => {
//     return res.json(`Successfully executed ${results.length} graindef sql queries.`);
//   });
// });

// export our router to be mounted by the parent application
module.exports = router
