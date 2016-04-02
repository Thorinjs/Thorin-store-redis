'use strict';
const Connection = require('./lib/connection'),
  async = require('async');
/**
 * Created by Adrian on 29-Mar-16.
 * Events:
 *  - reconnect({name, duration})
 *  - disconnect({name})
 */
module.exports = function init(thorin) {
  // Attach the Redis error parser to thorin.
  thorin.addErrorParser(require('./lib/errorParser'));

  const config = Symbol(),
    connections = Symbol();
  class ThorinRedisStore extends thorin.Interface.Store {
    static publicName() { return "redis"; }
    constructor() {
      super();
      this[config] = {};
      this[connections] = {
        'default': null,
        'publish': null,
        'subscribe': null
      }; // if we have a pub/sub one and the default one.
    }

    /*
    * Initializes the store.
    * */
    init(storeConfig) {
      this[config] = thorin.util.extend({
        debug: false,
        host: 'localhost',
        port: 6379,
        password: null,
        options: {} // custom redis options.
      }, storeConfig);
    }

    /*
    * Internal logger function that will log stuff.
    * */
    _log(command, args) {
      if(!this[config].debug) return;
      var str = 'Thorin.store.' + this.name + ': ' + command + ' ' + args.join(' ');
      // TODO: request a logger from thorin.
      console.info(str);
    }

    /*
    * Initializes the connections. By default, we only initialize the default
    * connection. The first time publish() or subscribe() is called, we initialize
    * the others.
    * */
    run(done) {
      this.createConnection('default', this[config], (err) => {
        if(err) {
          return done(thorin.error('REDIS.CONNECTION', 'Could not establish a connection to redis.', err));
        }
        done();
      });
    }

    /*
    * Closes all the connections programatically.
    * */
    stop(done) {
      this[connections].default.disconnect();
      if(this[connections].publish) {
        this[connections].publish.disconnect();
      }
      if(this[connections].subscribe) {
        this[connections].subscribe.disconnect();
      }
      done();
    }

    /*
    * Creates a new connection with the given data.
    * */
    createConnection(name, config, done) {
      if(this[connections][name]) {
        return done(thorin.error('REDIS.CONNECTION_EXISTS', 'A redis connection already exists with the name ' + name +'.'));
      }
      let conn = new Connection(name, config);
      conn.connect((e) => {
        if(e) return done(e);
        conn.startPing();
        done(null, conn);
      });
      let discTs = 0;
      conn.on('disconnect', (e) => {
        discTs = Date.now();
        this.emit('disconnect', {
          name: name,
          error: e
        });
      }).on('reconnect', (e) => {
        let data = {
          name: name
        };
        if(discTs > 0) {
          data.duration = Date.now() - discTs;
          discTs = 0;
        }
        this.emit('connect', data);
      });
      this[connections][name] = conn;
      return this;
    }

    /*
     * Checks if there is a connection.
     * Note: the named connection will default to "default",
     * Other values are "publish" or "subscribe"
     * */
    isConnected(_name) {
      if(!_name) _name = 'default';
      if(!this[connections][_name]) return false;
      return this[connections][_name].connected;
    }

    /*
    * Performs a publish() to the given channel.
    * Note: if no publish connection exists, we will create one automatically.
    * Returns a promise.
    * */
    publish(channel, data) {
      return new Promise((resolve, reject) => {
        if(typeof data === 'object' && data) {
          data = JSON.stringify(data);
        }
        var calls = [];
        /* init connection */
        if(!this[connections].publish) {
          calls.push((done) => {
            this.createConnection('publish', this[config], done);
          });
        }
        calls.push((done) => {
          if(!this.isConnected('publish')) {
            return done(thorin.error('REDIS.NOT_CONNECTED', 'The publisher connection is not ready yet.'));
          }
          this[connections].publish.connection.publish(channel, data, done);
        });
        async.series(calls, (e) => {
          this._log('publish', [channel, data]);
          if(e) return reject(thorin.error('REDIS.PUBLISH', 'Failed to publish to channel', e, 400));
          resolve();
        });
      });
    }

    /*
    * Subscribes to a channel with the given callback.
    * Note: if no subscriber connection exists, we will create one automatically.
    * Returns a promise.
    * */
    subscribe(channel, callback) {
      return new Promise((resolve, reject) => {
        var calls = [];
        /* init connection */
        if(!this[connections].subscribe) {
          calls.push((done) => {
            this.createConnection('subscribe', this[config], done);
          });
        }
        calls.push((done) => {
          if(!this.isConnected('subscribe')) {
            return done(thorin.error('REDIS.NOT_CONNECTED', 'The subscriber connection is not ready yet.'));
          }
          this[connections].subscribe.handleSubscribe(channel, callback);
          this[connections].subscribe.startSubscriber();
          done();
        });
        async.series(calls, (e) => {
          this._log('subscribe', [channel, callback.name]);
          if(e) return reject(thorin.error('REDIS.SUBSCRIBE', 'Failed to subscribe to channel', e));
          resolve();
        });
      });
    }

    /*
    * Unsubscribes from the given channel.
    * */
    unsubscribe(channel, _callback) {
      return new Promise((resolve, reject) => {
        if(!this[connections].subscribe) return resolve();
        this._log('unsubscribe', [channel]);
        try {
          this[connections].subscribe.handleUnsubscribe(channel, _callback);
        } catch(e) {
          return reject(thorin.error('REDIS.UNSUBSCRIBE', 'Failed to unsubscribe from channel', e));
        }
        resolve();
      });
    }

    /*
    * Runs any redis command, promisified.
    * */
    exec(command) {
      return new Promise((resolve, reject) => {
        if(!this.isConnected()) {
          return reject(thorin.error('REDIS.NOT_CONNECTED', 'The connection is not active yet.'));
        }
        command = command.toLowerCase();
        if(typeof this[connections].default.connection[command] !== 'function') {
          return reject(thorin.error('REDIS.COMMAND_NOT_FOUND', 'Invalid command issued: ' + command, 500));
        }
        let args = Array.prototype.slice.call(arguments);
        args.splice(0, 1);
        args.push((err, res) => {
          args.pop();
          this._log(command, [args]);
          if(err) {
            return reject(thorin.error('REDIS.EXEC', 'Redis command failed to execute.', err));
          }
          resolve(res);
        });
        this[connections].default.connection[command].apply(this[connections].default.connection, args);
      });
    }

    /*
    * Performs a multi() with multiple execs.
    * Syntax is:
    * var multi = redis.multi();
    * multi.exec('GET', 'myKey')
    * multi.exec('SET', 'somethingElse')
    * multi.commit().then((results) => {
    *   results[0] => GET myKey
    *   results[1] => SET myKey
    * });
    * */
    multi() {
      let self = this,
        connObj = self[connections].default.connection;
      const wrap = {};
      const cmds = [];
      /* Exec wrapper */
      wrap.exec = function DoExec() {
        let items = Array.prototype.slice.call(arguments);
        items[0] = items[0].toLowerCase();
        cmds.push(items);
        return wrap;
      };

      /* Commits the multi */
      wrap.commit = function DoCommit() {
        return new Promise((resolve, reject) => {
          if(!self.isConnected()) {
            return reject(thorin.error('REDIS.NOT_CONNECTED', 'Redis connection is not ready.'));
          }
          if(cmds.length === 0) return resolve();
          // check commands first.
          for(let i=0; i < cmds.length; i++) {
            let cmd = cmds[i][0];
            if(typeof connObj[cmd] !== 'function') {
              return reject(thorin.error('REDIS.COMMAND_NOT_FOUND', 'Invalid redis command:' + cmd, 500));
            }
          }
          var mObj = connObj.multi();
          cmds.forEach((item) => {
            let cmd = item.splice(0, 1)[0];
            self._log('multi: ' + cmd, [item]);
            mObj[cmd].apply(mObj, item);
          });
          mObj.exec((err, results) => {
            if(err) {
              return reject(thorin.error('REDIS.MULTI', 'Redis transaction encountered an error.', err));
            }
            resolve(results);
          });
        });
      };
      return wrap;
    }
  }

  return ThorinRedisStore;
};