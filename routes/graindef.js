const Router = require('express-promise-router')
const db = require('../db')
const util = require('./../util')
const graindefs = require('./../graindefs')
const router = new Router()

router.get('/', async (req, res, next) => {
  const graindefsJson = await util.readFile('./graindefs/graindefs.json')
  return res.json(graindefsJson.graindefs);
});

const getDimInfo = (member, dimKeys) => {
  return new Promise((resolve, reject) => {
    dimKeys = JSON.parse(dimKeys);
    // Get dimension info
    const dimInfo = dimKeys.find(dimKey => {
      return dimKey.dim_name === member.dimension;
    });

    if (!dimInfo) {
      reject('Could not find dimInfo for member.')
    }
    resolve(dimInfo)
  })
}

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
        return hierKey.name === member.hierarchy;
      });

      sqlParams.hierNumber = hierInfo.id;
      sqlParams.hierName = member.hierarchy;
    }
    
    // assemble the sql for the brick/block creation
    if (graindef.grainType === "brick" && member.memberSetType === "evaluated") {
      queryStrings = graindefs.makeGrainBrickQueryStrings(sqlParams);
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
      queryStrings = graindefs.makeGrainBlockQueryStrings(sqlParams, {"memberSet": member});
    }
    
    // TODO - queryStrings should not be an array
    if (queryStrings.length < 1) {
      return reject('No queryStrings returned.')
    }
    return resolve(queryStrings[0]);
  })
}

// router.post('/', async (req, res, next) => {
//   const graindefId = req.body.graindefId;
//   const graindefName = req.body.graindefName;
//   const graindefsJson = await util.readFile('./graindefs/graindefs.json')
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

router.patch('/', async (req, res, next) => {
  let results = null;
  const graindefString = req.body.graindef;
  
  // Check for multiple objects send? Should always be one with current implementation.
  const graindefJson = JSON.parse(graindefString)[0];
  
  // TODO - add support for multiple memberSets objects
  const memberSet = graindefJson.memberSets[0];
  const dimKeys = process.env.DIM_KEYS;
  const hierKeys = process.env.HIER_KEYS;
  
  // TODO - extract sql to graindefs/query.js
  results = await db.query(`
    INSERT INTO model.graindef (graindef_id, graindef_name, graindef_dimensions, graindef_graindef)
    VALUES (${graindefJson.id}, '${graindefJson.name}', '{${memberSet.dimension}}', '${graindefString}'::json)
    ON CONFLICT (graindef_id, graindef_name) DO 
    UPDATE SET
      graindef_id = EXCLUDED.graindef_id,
      graindef_dimensions = EXCLUDED.graindef_dimensions,
      graindef_graindef = EXCLUDED.graindef_graindef
    WHERE graindef.graindef_name = EXCLUDED.graindef_name;`, null, 'POSTGRESQL' );

  const dimInfo = await getDimInfo(memberSet, dimKeys)
  
  // For now, all graindefs are going into 'model' schema
  const graindefSql = await createGrainDefSql(graindefJson, memberSet, dimInfo, hierKeys, 'model')

  results = await db.query(graindefSql, null, 'POSTGRESQL');
  
  return res.json(`Successfully executed sql to create graindef ${graindefJson.name}.`);
});

// export our router to be mounted by the parent application
module.exports = router
