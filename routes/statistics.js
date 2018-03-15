const Router = require('express-promise-router')
const db = require('../db')
const util = require('./../util')
const router = new Router()

router.get('/', async (req, res, next) => {
  const manifestType = req.query.manifestType;
  const tenant = req.query.tenant;

  if (!manifestType) {
    return res.status(400).json({ error: 'You must supply a manifest type' });
  }

  const stats = await util.readJsonFile(`./manifests/statistics.json`)
  
  return res.json({
    "manifestStats": stats.manifests[manifestType],
    "globalStats": stats.global.find(global => { return global.tenant === tenant}),
    "startTime": new Date()
  });
});

// export our router to be mounted by the parent application
module.exports = router
