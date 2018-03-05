const graindef = require('./graindef.js')
const grid = require('./grid.js')
const connection = require('./connection.js')
const manifest = require('./manifest.js')
const statistics = require('./statistics.js')

module.exports = (app) => {
  app.use('/graindef', graindef)
  app.use('/grid', grid)
  app.use('/connection', connection)
  app.use('/manifest', manifest)
  app.use('/statistics', statistics)
}
