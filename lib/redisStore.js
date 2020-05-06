'use strict';
const initConnection = require('./connection');

module.exports = function (thorin, opt) {


  const Connection = initConnection(thorin, opt),
    logger = thorin.logger(opt.name || 'redis');

  class ThorinRedisStore extends thorin.Interface.Store {

    #config = {};
    #connections = {};
    #retryCounter = null;
    #retryConnect = null;

    static publicName() {
      return "redis";
    }

    constructor() {
      super();
      ['_events', '_eventsCount', '_maxListeners'].forEach((n) => {
        Object.defineProperty(this, n, {
          value: this[n],
          enumerable: false,
          writable: true
        });
      });
      this.type = "redis";
      this.#config = {};
      this.#connections = {
        'default': null,
        'publish': null,
        'subscribe': null
      }; // if we have a pub/sub one and the default one.
    }

    /**
     * Initializes the store.
     * */
    init(storeConfig) {
      this.#config = thorin.util.extend({
        required: true, // if set to false, will not stop app on boot time.
        debug: false,
        host: 'localhost',
        port: 6379,
        password: null,
        namespace: 'rns:', // the namespace that we'll use when calling redis.key('myKey') => rns:myKey
        dynamic_reconnect: true,    //always re-initiate the redis client with the hostname. This is useful when using docker or something with dynamic IP address attached to the hostname
        retry: false,               // Works with required=false, to always retry connection
        retryInterval: 10000,       // Works with retry=true, to try and re-connect
        clustered: false,           // if set to true, we will query the cluster with CLUSTER NODES to connect to master.
        sentinel: null,             // if set to an [{host, port}], ioredis will use it to talk to sentinels.
        name: null,               // If using sentinel, this is the name of the redis sentinel cluster (defaults to redis)
        options: {
          connect_timeout: 10000
        } // custom redis options.
      }, storeConfig);
      logger.name = this.name;
      thorin.config('store.' + this.name, this.#config);
    }

    /**
     * Builds a key by appending the store's namespace prefix.
     * */
    key(name) {
      if (typeof this.#config.namespace !== 'string') return name;
      if (typeof name === 'object') return name;
      return this.#config.namespace + name;
    }

    /**
     * Initializes the connections. By default, we only initialize the default
     * connection. The first time publish() or subscribe() is called, we initialize
     * the others.
     * */
    run(done) {
      this.createConnection('default', this.#config, (err) => {
        if (err) {
          logger.warn(`Could not initiate redis connection ${this.name}`);
          if (this.#config.retry) {
            this.retryConnect();
          }
          if (this.#config.required) {
            return done(thorin.error('REDIS.CONNECTION', 'Could not establish a connection to redis.', err));
          }
        }
        done();
      });
    }

    /**
     * Tries to retry connection to redis
     * */
    retryConnect() {
      let cObj = this.#connections.default;
      if (!cObj) return;
      let retryInterval = this.#config.retryInterval || 10000;
      if (!this.#retryCounter) this.#retryCounter = 1;
      if (this.#retryConnect) clearTimeout(this.#retryConnect);
      if (this.#config.debug) {
        logger.trace(`Re-trying to connect to redis [${this.#retryCounter}]`);
      }
      cObj.connect((e) => {
        if (!e) {
          logger.info(`Re-connected to redis`);
          this.#retryConnect = null;
          this.#retryCounter = null;
          return;
        }
        this.#retryCounter++;
        this.#retryConnect = setTimeout(() => {
          this.retryConnect();
        }, retryInterval);
      });
    }

    /**
     * Closes all the connections programatically.
     * */
    stop(done) {
      Object.keys(this.#connections).forEach((n) => {
        this.#connections[n] && this.#connections[n].disconnect();
      });
      done && done();
    }

    /**
     * Creates or returns a raw redis connection.
     * */
    getConnection(name, _opt, _done) {
      let opt = (typeof _opt === 'object' && _opt) || {},
        done = (typeof _opt === 'function' ? _opt : _done);
      if (this.#connections[name]) {
        if (typeof done === 'function') {
          return done(null, this.#connections[name].connection);
        }
        return Promise.resolve(this.#connections[name].connection);
      }
      opt = thorin.util.extend(this.#config, opt);
      if (typeof done === 'function') {
        return this.createConnection(name, opt, (e, conn) => {
          if (e) return done(e);
          done(null, conn.connection);
        });
      }
      return new Promise((resolve, reject) => {
        this.createConnection(name, opt, (e, conn) => {
          if (e) return reject(e);
          resolve(conn.connection);
        });
      });
    }

    /**
     * Creates a new connection with the given data.
     * */
    createConnection(name, connectionConfig, done) {
      if (typeof connectionConfig !== 'object' || !connectionConfig) connectionConfig = this.#config;
      if (this.#connections[name]) {
        let err = thorin.error('REDIS.CONNECTION_EXISTS', 'A redis connection already exists with the name ' + name);
        if (done) return done(err);
        return Promise.reject(err);
      }
      let conn = new Connection(name, connectionConfig);
      this.#connections[name] = conn;
      if (done) {
        this.#bindConnection(conn, done);
        return this;
      }
      return new Promise((resolve, reject) => {
        this.#bindConnection(conn, (e) => {
          if (e) return reject(e);
          resolve(conn);
        });
      });
    }

    /**
     * Checks if there is a connection.
     * Note: the named connection will default to "default",
     * Other values are "publish" or "subscribe"
     * */
    isConnected(_name = 'default') {
      if (!this.#connections[_name]) return false;
      return this.#connections[_name].connected;
    }

    /**
     * Performs a publish() to the given channel.
     * Note: if no publish connection exists, we will create one automatically.
     * Returns a promise.
     * */
    async publish(channel, data) {
      try {
        if (typeof data === 'object' && data) {
          data = JSON.stringify(data);
        }
        // init connection
        let connObj = this.#connections.publish;
        if (!connObj) {
          connObj = await this.createConnection('publish');
        }
        // IF the connection is not active and there is no connecting, we reject.
        if (!connObj.connecting && !connObj.connected) {
          throw thorin.error('REDIS.NOT_CONNECTED', 'The publisher connection is not ready yet.');
        }
        // IF the connection is starting, we wait for it to complete.
        if (connObj.connecting) {
          return new Promise((resolve, reject) => {
            connObj.onConnect(() => {
              connObj.connection.publish(channel, data, (e) => {
                if (e) return reject(e);
                resolve();
              });
            });
          });
        }
        // otherwise, we send
        let res = await connObj.connection.publish(channel, data);
        connObj.emit('publish', channel, data);
        return res;
      } catch (e) {
        if (e.ns === 'REDIS') throw e;
        throw thorin.error('REDIS.PUBLISH', 'Could not publish message to channel', e, 400);
      }
    }

    /**
     * Subscribes to a channel with the given callback.
     * Note: if no subscriber connection exists, we will create one automatically.
     * Returns a promise.
     * */
    async subscribe(channel, callback) {
      try {
        let connObj = this.#connections.subscribe;
        if (!connObj) {
          connObj = await this.createConnection('subscribe')
        }
        // IF the connection is not active and there is no connecting, we reject.
        if (!connObj.connecting && !connObj.connected) {
          throw thorin.error('REDIS.NOT_CONNECTED', 'The subscriber connection is not ready yet.');
        }
        // IF the connection is starting, we wait for it to complete.
        if (connObj.connecting) {
          return new Promise((resolve) => {
            connObj.onConnect(() => {
              connObj.handleSubscribe(channel, callback);
              connObj.startSubscriber();
              resolve();
            });
          });
        }
        connObj.handleSubscribe(channel, callback);
        connObj.startSubscriber();
        return true;
      } catch (e) {
        if (e.ns === 'REDIS') throw e;
        throw thorin.error('REDIS.SUBSCRIBE', 'Could not subscribe to channel', e, 400);
      }
    }

    /**
     * Unsubscribes from the given channel.
     * */
    async unsubscribe(channel, _callback) {
      try {
        let connObj = this.#connections.subscribe;
        if (!connObj) return true;
        connObj.handleUnsubscribe(channel, _callback);
        return true;
      } catch (e) {
        if (e.ns === 'REDIS') throw e;
        throw thorin.error('REDIS.UNSUBSCRIBE', 'Could not unsubscribe from channel', e, 400);
      }
    }

    /**
     * Runs any redis command, promisified.
     * */
    exec(command) {
      let args = (command instanceof Array ? command : [...arguments]);
      let callbackFn = args[args.length - 1];
      // call with this.
      if (typeof callbackFn === 'function') {
        args.pop();
        args.splice(0, 0, callbackFn);
        this.#doExec(args);
        return this;
      }
      return new Promise((resolve, reject) => {
        args.splice(0, 0, (e, res) => {
          if (e) return reject(e);
          resolve(res);
        });
        this.#doExec(args);
      });
    }

    /**
     * Wrapper over the thorin exec function, to allow both promise based
     * and callback based.
     * */
    #doExec = (args) => {
      let callback = args[0],
        command = args[1];
      args.splice(0, 2);  //remove the first cb and command
      if (!this.isConnected()) {
        return callback(thorin.error('REDIS.NOT_CONNECTED', 'The connection is not active yet.'));
      }
      command = command.toLowerCase();
      if (typeof this.#connections.default.connection[command] !== 'function') {
        return callback(thorin.error('REDIS.COMMAND_NOT_FOUND', 'Invalid command issued: ' + command, 500));
      }
      args.push((err, res) => {
        if (err) {
          return callback(thorin.error('REDIS.EXEC', 'Redis command failed to execute.', err));
        }
        if (command === 'hgetall' && typeof res === 'object' && res) {
          // backward compatibility
          if (Object.keys(res).length === 0) {
            res = null;
          }
        }
        callback(null, res);
      });
      let connObj = this.#connections.default;
      connObj.emit('command', command, args);
      connObj.connection[command].apply(connObj.connection, args);
    }

    /**
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
      let connObj = this.#connections.default.connection;
      const wrap = {};
      const cmds = [];
      /* Exec wrapper */
      wrap.exec = function DoExec(items) {
        items = (items instanceof Array ? items : [...arguments]);
        items[0] = items[0].toLowerCase();
        cmds.push(items);
        return wrap;
      };

      /* Commits the multi */
      wrap.commit = (callbackFn) => {
        if (typeof callbackFn === 'function') {
          this.#doMulti(connObj, cmds, callbackFn);
          return wrap;
        }
        return new Promise((resolve, reject) => {
          this.#doMulti(connObj, cmds, (e, res) => {
            if (e) return reject(e);
            resolve(res);
          });
        });
      };
      return wrap;
    }

    /**
     * Wrapper over the thorin multi commit(), to allow both promise and async based calls.
     * */
    #doMulti = (connObj, cmds, callback) => {
      if (!this.isConnected()) {
        return callback(thorin.error('REDIS.NOT_CONNECTED', 'Redis connection is not ready.', 502));
      }
      if (cmds.length === 0) return callback();
      // check commands first.
      for (let i = 0; i < cmds.length; i++) {
        let cmd = cmds[i][0];
        if (typeof connObj[cmd] !== 'function') {
          return callback(thorin.error('REDIS.COMMAND_NOT_FOUND', 'Invalid redis command:' + cmd, 500));
        }
      }
      let mObj = connObj.multi();
      cmds.forEach((item) => {
        let cmd = item.splice(0, 1)[0];
        mObj[cmd].apply(mObj, item);
        item.splice(0, 0, cmd);
      });
      this.#connections.default.emit('multi', cmds);
      mObj.exec((err, results) => {
        if (err) {
          return callback(thorin.error('REDIS.MULTI', 'Redis transaction encountered an error.', err, 500));
        }
        let finalResults = [];
        for (let i = 0, len = results.length; i < len; i++) {
          let t = results[i];
          if (t[0] === null) {
            finalResults.push(t[1]);
          }
        }
        callback(null, finalResults);
      });
    }

    /**
     * Creates an internal connection, and handles its events.
     * */
    #bindConnection = (conn, done) => {
      this.#connectionLogger(conn);
      conn.connect((e) => {
        if (e) return done(e);
        conn.connecting = false;
        conn.startPing();
        this.emit('connect', {
          name: conn.name
        });
        done(null, conn);
      });
      let discTs = 0;
      conn.on('disconnect', (e) => {
        discTs = Date.now();
        this.emit('disconnect', {
          name: conn.name,
          error: e
        });
      }).on('reconnect', () => {
        let data = {
          name: conn.name,
        };
        if (discTs > 0) {
          data.duration = Date.now() - discTs;
          discTs = 0;
        }
        this.emit('connect', data);
      }).on('error', (e) => {
        if (this.#config.debug) {
          logger.warn(`Connection [${conn.name}] encountered an error`);
          logger.trace(e);
        }
      });
      return conn;
    }

    /**
     * Internal function that will listen for connection events.
     * */
    #connectionLogger = (conObj) => {
      if (this.#config.debug === false) return;

      let connectionName = conObj.name === 'default' ? '' : conObj.name + ': ';
      conObj
        .on('info', (msg) => {
          logger.info(connectionName + ': ' + msg);
        })
        .on('warning', (msg) => {
          logger.warn(connectionName + ': ' + (msg || 'encountered a warning'));
        })
        .on('connect', () => {
          logger.info(connectionName + 'Connected to redis server');
        })
        .on('disconnect', () => {
          logger.warn(connectionName + 'Disconnected from redis server');
        })
        .on('reconnect', () => {
          logger.info(connectionName + 'Reconnected to redis server');
        })
        .on('close', () => {
          logger.info(connectionName + 'Redis connection closed.');
        })
        .on('subscribe.message', (channel, msg) => {
          logger.trace(connectionName + 'Received message on channel [%s]', channel, msg);
        })
        .on('subscribe.channel', (channel) => {
          logger.trace(connectionName + 'Subscribed to channel [%s]', channel);
        })
        .on('unsubscribe', (channel) => {
          logger.trace(connectionName + 'Unsubscribed from channel [%s]', channel);
        })
        .on('publish', (channel, msg) => {
          logger.trace(connectionName + 'Publish to channel [%s]', channel, msg);
        })
        .on('command', (cmd, cmdArgs) => {
          let items = [cmd.toUpperCase()];
          cmdArgs.forEach((i) => {
            if (typeof i === 'function') return;
            items.push(i);
          });
          logger.trace(connectionName + 'Execute: ' + '"' + items.join(' ') + '"');
        })
        .on('multi', (cmdArgs) => {
          let items = [];
          cmdArgs.forEach((item) => {
            let tmp = [];
            item.forEach((i) => {
              if (typeof i === 'function') return;
              tmp.push(i);
            });
            items.push('"' + tmp.join(' ') + '"');
          });
          for (let i = 0; i < items.length; i++) {
            logger.trace(connectionName + ' MULTI: ' + items[i]);
          }
        });
    }

  }


  return ThorinRedisStore;
}
