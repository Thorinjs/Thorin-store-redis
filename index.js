'use strict';
const initStore = require('./lib/redisStore');
/**
 * Created by Adrian on 29-Mar-16.
 * Events:
 *  - reconnect({name, duration})
 *  - disconnect({name})
 */
module.exports = function init(thorin, opt) {
  const async = thorin.util.async;
  // Attach the Redis error parser to thorin.
  thorin.addErrorParser(require('./lib/errorParser'));

  const ThorinRedisStore = initStore(thorin, opt);

  return ThorinRedisStore;
};
module.exports.publicName = 'redis';