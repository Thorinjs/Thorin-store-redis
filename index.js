'use strict';
const initStore = require('./lib/redisStore'),
  initErrorParser = require('./lib/errorParser');
/**
 * Created by Adrian on 29-Mar-16.
 * Events:
 *  - reconnect({name, duration})
 *  - disconnect({name})
 */
module.exports = function init(thorin, opt) {
  // Attach the Redis error parser to thorin.
  thorin.addErrorParser(initErrorParser);

  const ThorinRedisStore = initStore(thorin, opt);

  return ThorinRedisStore;
};
module.exports.publicName = 'redis';
