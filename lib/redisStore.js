'use strict';
const Connection = require('./connection');

module.exports = function(thorin, opt) {


  const config = Symbol(),
    connections = Symbol();
  class ThorinRedisStore extends thorin.Interface.Store {
    static publicName() {
      return "redis";
    }

    constructor() {
      super();
      this.type = "redis";
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
        required: true, // if set to false, will not stop app on boot time.
        debug: false,
        host: 'localhost',
        port: 6379,
        password: null,
        namespace: 'rns:', // the namespace that we'll use when calling redis.key('myKey') => rns:myKey
        options: {
          connect_timeout: 10000
        } // custom redis options.
      }, storeConfig);
      thorin.config('store.' + this.name, this[config]);
    }

    /*
     * Builds a key by appending the store's namespace prefix.
     * */
    key(name) {
      if(typeof this[config].namespace !== 'string') return name;
      if(typeof name === 'object') return name;
      return this[config].namespace + name;
    }

    /*
     * Initializes the connections. By default, we only initialize the default
     * connection. The first time publish() or subscribe() is called, we initialize
     * the others.
     * */
    run(done) {
      this.createConnection('default', this[config], (err) => {
        if (err) {
          const logger = thorin.logger(this.name);
          logger.warn(`Could not initiate redis connection ${this.name}`);
          if(this[config].required) {
            return done(thorin.error('REDIS.CONNECTION', 'Could not establish a connection to redis.', err));
          }
        }
        done();
      });
    }

    /*
     * Closes all the connections programatically.
     * */
    stop(done) {
      this[connections].default.disconnect();
      if (this[connections].publish) {
        this[connections].publish.disconnect();
      }
      if (this[connections].subscribe) {
        this[connections].subscribe.disconnect();
      }
      done();
    }

    /*
    * Creates or returns a raw redis connection.
    * */
    getConnection(name, _opt, _done) {
      let opt = (typeof _opt === 'object' && _opt) || {},
        done = (typeof _opt === 'function' ? _opt : _done);
      if(this[connections][name]) {
        if(typeof done === 'function') {
          return done(null, this[connections][name].connection);
        }
        return Promise.resolve(this[connections][name].connection);
      }
      opt = thorin.util.extend(this[config], opt);
      if(typeof done === 'function') {
        return this.createConnection(name, opt, (e, conn) => {
          if(e) return done(e);
          done(null, conn.connection);
        });
      }
      return new Promise((resolve, reject) => {
        this.createConnection(name, opt, (e, conn) => {
          if(e) return reject(e);
          resolve(conn.connection);
        });
      });
    }

    /*
     * Creates a new connection with the given data.
     * */
    createConnection(name, connectionConfig, done) {
      if (this[connections][name]) {
        return done(thorin.error('REDIS.CONNECTION_EXISTS', 'A redis connection already exists with the name ' + name + '.'));
      }
      let conn = new Connection(name, connectionConfig);
      if(this[config].debug) {
        connectionLogger.call(this, conn);
      }
      conn.connect((e) => {
        if (e) return done(e);
        conn.connecting = false;
        conn.startPing();
        this.emit('connect', {
          name: name
        });
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
        if (discTs > 0) {
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
      if (!_name) _name = 'default';
      if (!this[connections][_name]) return false;
      return this[connections][_name].connected;
    }

    /*
     * Performs a publish() to the given channel.
     * Note: if no publish connection exists, we will create one automatically.
     * Returns a promise.
     * */
    publish(channel, data) {
      return new Promise((resolve, reject) => {
        if (typeof data === 'object' && data) {
          data = JSON.stringify(data);
        }
        var calls = [];
        /* init connection */
        if (!this[connections].publish) {
          calls.push((done) => {
            this.createConnection('publish', this[config], done);
          });
        }
        calls.push((done) => {
          /* IF the connection is not active and there is no connecting, we reject. */
          if(!this[connections].publish.connecting && !this.isConnected('publish')) {
            return done(thorin.error('REDIS.NOT_CONNECTED', 'The publisher connection is not ready yet.'));
          }
          /* IF the connection is starting, we wait for it to complete. */
          if(this[connections].publish.connecting) {
            this[connections].publish.onConnect(() => {
              this[connections].publish.connection.publish(channel, data, done);
            });
            return;
          }
          // otherwise, we send
          this[connections].publish.connection.publish(channel, data, done);
        });
        async.series(calls, (e) => {
          this[connections].publish.emit('publish', channel, data);
          if (e) return reject(thorin.error('REDIS.PUBLISH', 'Failed to publish to channel', e, 400));
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
        if (!this[connections].subscribe) {
          calls.push((done) => {
            this.createConnection('subscribe', this[config], done);
          });
        }
        calls.push((done) => {
          /* IF the connection is not active and there is no connecting, we reject. */
          if(!this[connections].subscribe.connecting && !this.isConnected('subscribe')) {
            return done(thorin.error('REDIS.NOT_CONNECTED', 'The subscriber connection is not ready yet.'));
          }
          /* IF the connection is starting, we wait for it to complete. */
          if(this[connections].subscribe.connecting) {
            this[connections].subscribe.onConnect(() => {
              this[connections].subscribe.handleSubscribe(channel, callback);
              this[connections].subscribe.startSubscriber();
            });
            return;
          }
          this[connections].subscribe.handleSubscribe(channel, callback);
          this[connections].subscribe.startSubscriber();
          done();
        });
        async.series(calls, (e) => {
          if (e) return reject(thorin.error('REDIS.SUBSCRIBE', 'Failed to subscribe to channel', e));
          resolve();
        });
      });
    }

    /*
     * Unsubscribes from the given channel.
     * */
    unsubscribe(channel, _callback) {
      return new Promise((resolve, reject) => {
        if (!this[connections].subscribe) return resolve();
        try {
          this[connections].subscribe.handleUnsubscribe(channel, _callback);
        } catch (e) {
          return reject(thorin.error('REDIS.UNSUBSCRIBE', 'Failed to unsubscribe from channel', e));
        }
        resolve();
      });
    }

    /*
     * Runs any redis command, promisified.
     * */
    exec(command) {
      let args = Array.prototype.slice.call(arguments),
        callbackFn = args[args.length-1];
      // call with this.
      if(typeof callbackFn === 'function') {
        args.pop();
        args.splice(0,0, callbackFn);
        doExec.apply(this, args);
        return;
      }
      return new Promise((resolve, reject) => {
        args.splice(0, 0, (e, res) => {
          if(e) return reject(e);
          resolve(res);
        });
        doExec.apply(this, args);
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
      wrap.exec = function DoExec(items) {
        items = (items instanceof Array ? items : Array.prototype.slice.call(arguments));
        items[0] = items[0].toLowerCase();
        cmds.push(items);
        return wrap;
      };

      /* Commits the multi */
      wrap.commit = function DoCommit(callbackFn) {
        // we do it with callback.
        if(typeof callbackFn === 'function') {
          doMulti.call(self, connObj, cmds, callbackFn);
          return wrap;
        }
        return new Promise((resolve, reject) => {
          doMulti.call(self, connObj, cmds, (e, res) => {
            if(e) return reject(e);
            resolve(res);
          });
        });
      };
      return wrap;
    }
  }

  /*
   * Wrapper over the thorin exec function, to allow both promise based
   * and callback based.
   * */
  function doExec(callback, command) {
    let args = Array.prototype.slice.call(arguments);
    args.splice(0, 2);  //remove the first cb and command
    if (!this.isConnected()) {
      return callback(thorin.error('REDIS.NOT_CONNECTED', 'The connection is not active yet.'));
    }
    command = command.toLowerCase();
    if (typeof this[connections].default.connection[command] !== 'function') {
      return callback(thorin.error('REDIS.COMMAND_NOT_FOUND', 'Invalid command issued: ' + command, 500));
    }
    args.push((err, res) => {
      if (err) {
        return callback(thorin.error('REDIS.EXEC', 'Redis command failed to execute.', err));
      }
      callback(null, res);
    });
    this[connections].default.emit('command',command, args);
    this[connections].default.connection[command].apply(this[connections].default.connection, args);
  }

  /*
   * Wrapper over the thorin multi commit(), to allow both promise and async based calls.
   * */
  function doMulti(connObj, cmds, callback) {
    if(!this.isConnected()) {
      return callback(thorin.error('REDIS.NOT_CONNECTED', 'Redis connection is not ready.', 500));
    }
    if(cmds.length === 0) return callback();
    // check commands first.
    for (let i = 0; i < cmds.length; i++) {
      let cmd = cmds[i][0];
      if (typeof connObj[cmd] !== 'function') {
        return callback(thorin.error('REDIS.COMMAND_NOT_FOUND', 'Invalid redis command:' + cmd, 500));
      }
    }
    var mObj = connObj.multi();
    cmds.forEach((item) => {
      let cmd = item.splice(0, 1)[0];
      mObj[cmd].apply(mObj, item);
      item.splice(0, 0, cmd);
    });
    this[connections].default.emit('multi', cmds);
    mObj.exec((err, results) => {
      if (err) {
        return callback(thorin.error('REDIS.MULTI', 'Redis transaction encountered an error.', err, 500));
      }
      callback(null, results);
    });
  }

  /*
   * Internal function that will listen for connection events.
   * */
  function connectionLogger(conObj) {
    let logger = thorin.logger(this.name),
      connectionName = conObj.name === 'default' ? '' : conObj.name + ': ';
    conObj.on('connect', () => {
      logger.info(connectionName + 'Connected to redis server');
    }).on('disconnect', () => {
      logger.warn(connectionName + 'Disconnected from redis server');
    }).on('reconnect', () => {
      logger.info(connectionName + 'Reconnected to redis server');
    }).on('close', () => {
      logger.info(connectionName + 'Redis connection closed.');
    }).on('subscribe.message', (channel, msg) => {
      logger.trace(connectionName + 'Received message on channel [%s]', channel, msg);
    }).on('subscribe.channel', (channel) => {
      logger.trace(connectionName + 'Subscribed to channel [%s]', channel);
    }).on('unsubscribe', (channel) => {
      logger.trace(connectionName + 'Unsubscribed from channel [%s]', channel);
    }).on('publish', (channel, msg) => {
      logger.trace(connectionName + 'Publish to channel [%s]', channel, msg);
    }).on('command', (cmd, cmdArgs) => {
      let items = [cmd.toUpperCase()];
      cmdArgs.forEach((i) => {
        if(typeof i === 'function') return;
        items.push(i);
      });
      logger.trace(connectionName + 'Execute: ' + '"' + items.join(' ') + '"');
    }).on('multi', (cmdArgs) => {
      let items = [];
      cmdArgs.forEach((item) => {
        let tmp = [];
        item.forEach((i) => {
          if(typeof i === 'function') return;
          tmp.push(i);
        });
        items.push('"' + tmp.join(' ') + '"');
      });
      for(let i=0; i < items.length; i++) {
        logger.trace(connectionName + ' MULTI: ' + items[i]);
      }
    });
  }

  return ThorinRedisStore;
}