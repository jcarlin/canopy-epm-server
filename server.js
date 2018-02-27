require('dotenv').config();
const fs = require('fs');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');
const async = require('async');


const mountAllRoutes = require('./routes')
const port = process.env.PORT || 8080;
const app = express();

// Necessary for express to be able to read the request body
app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true
  })
);

// Allow cross-origin resource sharing
// TODO - limit this to your own domain
// for prod use
app.use(cors());

const checkJwt = jwt({
  secret: jwksRsa.expressJwtSecret({
    cache: true,
    rateLimit: true,
    jwksRequestsPerMinute: 5,
    jwksUri: `https://${process.env.AUTH0_ISSUER}/.well-known/jwks.json`
  }),
  audience: `${process.env.AUTH0_AUDIENCE}`,
  issuer: `https://${process.env.AUTH0_ISSUER}/`,
  algorithms: ['RS256']
});

// Use the checJwt middleware defined
// to ensure the user sends a valid
// access token produced by Auth0
// app.use(checkJwt);

// Wires up all the routes exported in routes/index.js
mountAllRoutes(app);

// Move this to ./routes?
const errorHandler = (err, req, res, next) => {
  console.log('errorHandler err: ', err);
  res.status(500);
  return res.json({ error: err.message });
};

// Error Handler
app.use(errorHandler);

app.listen(port);
console.log(`Express app started on port ${port} using ${process.env.DB_TYPE} database.`);

