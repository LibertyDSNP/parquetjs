const { setupServer } = require('msw/node');
const handlers = require('./handlers');

module.exports = setupServer(...handlers);
