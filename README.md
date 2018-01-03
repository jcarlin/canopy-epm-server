# Canopy EPM Server

## `.env` File

A `.env` file is used to hold environment-specific keys such as a database
password. This file should not be committed to souce control.

Copy the `.env.example` file provided at the root of the project and rename it
to `.env`. Populate it with an necessary variables such as the database
password.

## Installation

Install the dependencies in the `packege.json` file.

```bash
npm install
```

## Running the Server

```bash
node server.js
```

To run the server with live reload to watch changes:

```bash
npm run dev
```

## Deployment

To deploy, first commit and checkin changes, then run:

```
eb deploy
```

You will need to have the
[Elastic Beanstalk CLI](http://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3.html)
installed.

## License

UNLICENSED

## Git Workflow

TO DO
