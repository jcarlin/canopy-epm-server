const Router = require('express-promise-router')
const db = require('../db')
const util = require('./../util')
const router = new Router()

// Utility endpoint which you can send
// a manifest to and receive the column and row defs
// produce by running it through `buildTableData`
// app.post('/test-manifest', (req, res, next) => {
//   try {
//     if (!req.body.manifest) {
//       return res.status(400).json({
//         error:
//           'You must supply a manifest. Send it on an object with a `manfifest` key: { manifest: ... }'
//       });
//     }

//     const tableData = buildTableData(req.body.manifest);
//     return res.json(tableData);
//   } catch(err) {
//     return next(err);
//   }
// });

router.get('/', async (req, res, next) => {
  const manifestType = req.query.manifestType;
  if (!manifestType) {
    return res.status(400).json({ error: 'You must supply a manifest type' });
  }

  const manifest = await util.readFile(`./manifests/${manifestType}.json`)
  return res.json({manifest});
});

// export our router to be mounted by the parent application
module.exports = router
