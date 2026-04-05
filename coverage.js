'use strict';
// Generates coverage report after nyc has collected data.
// The nyc 15 CLI report is broken with Node 22, so we use the programmatic API.
const NYC = require('./node_modules/nyc');
const nyc = new NYC({
  cwd: __dirname,
  include: ['dist-test/src/**/*.js'],
  exclude: ['dist-test/src/example.js', 'dist-test/src/index.js'],
  reporter: ['text', 'html'],
});
nyc.report().catch(err => { console.error(err); process.exit(1); });
