'use strict';
const Redis = require('ioredis'),
  EventEmitter = require('events').EventEmitter;

/**
 * EVENTS:
 *  disconnect
 *  reconnect
 *  connect
 *  warning
 *  close
 *  subscribe.message(channel, msg)
 *  subscribe.channel(channel)
 *  unsubscribe(channel)
 *  publish(channel, msg)
 *  command(cmdArgs)
 *  multi(cmdArgs)
 */
const PING_TIMER = 50 * 1000;
module.exports = function init(thorin) {

  class RedisConnection extends EventEmitter {

    #connection = null;
    #subscribers = null;
    #subscriberStarted = false;
    #config = null;
    #ping = null;

    constructor(name, config) {
      super();
      this.setMaxListeners(Infinity);
      ['_events', '_eventsCount', '_maxListeners'].forEach((n) => {
        Object.defineProperty(this, n, {
          value: this[n],
          enumerable: false,
          writable: true
        });
      });
      this.name = name;
      this.#config = config;
      this.connected = false;
      this.connecting = true;
      this.wasConnected = false;
      this._onConnect = [];
      if (this.#config.dynamic_reconnect) {
        if (!this.#config.options) this.#config.options = {};
        this.#config.options.retry_strategy = () => undefined;
      }
    }

    get subscribers() {
      return this.#subscribers;
    }

    get subscriberStarted() {
      return this.#subscriberStarted;
    }

    get connection() {
      return this.#connection;
    }

    set connection(v) {
      if (!this.#connection) this.#connection = v;
    }

    /**
     * Callback that is registered to be called once the connection is established.
     * */
    onConnect(fn) {
      if (this.connected) return fn();
      this._onConnect.push(fn);
    }

    /**
     * Handles dynamic reconnection, by re-initializing the redis connection
     * */
    dynamicReconnect() {
      if (this._reconnectTimer) {
        clearTimeout(this._reconnectTimer);
      }
      this._reconnectTimer = setTimeout(() => {
        if (this.connection) {
          this.connection.removeAllListeners();
        }
        this.connect((err) => {
          if (err) {
            this.dynamicReconnect();
          }
        });
      }, 1000);
    }

    /**
     * Connects to redis.
     * */
    connect(done, _fromCluster) {
      if (typeof this.#config.password === 'string' && this.#config.password.trim() !== '') {
        this.#config.options['auth_pass'] = this.#config.password;
      }
      Redis.debug_mode = false;
      let _opt = {
        port: this.#config.port,
        host: this.#config.host,
        connectTimeout: this.#config.timeout || 4000
      };
      if (this.#config.password) {
        _opt.password = this.#config.password;
      }
      if (this.#config.options) {
        _opt = thorin.util.extend(_opt, this.#config.options);
      }
      if (this.#config.sentinel) {
        let items = [];
        if (typeof this.#config.sentinel === 'string') {
          let tmp = this.#config.sentinel.split(':');
          let h = tmp[0],
            p = tmp[1] || '26379';
          items.push({
            host: h,
            port: parseInt(p, 10)
          });
        } else if (this.#config.sentinel instanceof Array) {
          for (let i = 0; i < this.#config.sentinel.length; i++) {
            let item = this.#config.sentinel[i];
            if (typeof item === 'string') {
              let tmp = item.split(':');
              let h = tmp[0],
                p = tmp[1] || '26379';
              items.push({
                host: h,
                port: parseInt(p, 10)
              });
            } else if (typeof this.#config.sentinel[i] === 'object' && this.#config.sentinel[i]) {
              items.push(this.#config.sentinel[i]);
            }
          }
        } else if (typeof this.#config.sentinel === 'object' && this.#config.sentinel) {
          items.push(this.#config.sentinel);
        }
        _opt.sentinels = items;
        _opt.name = this.#config.name;
        if (!_opt.name) _opt.name = 'redis';
      }
      if (this.connection) {
        this.connection.removeAllListeners('error');
        this.connection.removeAllListeners('ready');
      } else {
        this.connection = new Redis(_opt);
      }
      let isDoneCalled = false,
        isDisconnected = false;
      this.connection.on('error', (e) => {
        this.#subscriberStarted = false;
        this.connected = false;
        this.connecting = true;
        if (!isDisconnected) {
          isDisconnected = true;
          if (isDoneCalled) {
            this.emit('disconnect', e);
            if (this.#config.dynamic_reconnect) {
              this.dynamicReconnect();
            }
            return;
          }
        }
        if (!isDoneCalled) {
          isDoneCalled = true;
          return done(e);
        }
      });
      let self = this;

      function onReady() {
        self.connected = true;
        self.connecting = false;
        if (isDisconnected) {
          isDisconnected = false;
        }
        if (self.wasConnected) {
          self.emit('reconnect');
          self.reSubscribe();
        } else {
          self.wasConnected = true;
          self.emit('connect');
        }
        let onConnectLength = self._onConnect.length;
        if (onConnectLength.length > 0) {
          for (let i = 0; i < onConnectLength; i++) {
            self._onConnect[i]();
          }
          self._onConnect = [];
        }
        if (!isDoneCalled) {
          isDoneCalled = true;
          return done();
        }
      }

      this.connection.on('ready', () => {
        if (this.#config.clustered === false || _fromCluster === true) {
          return onReady();
        }
        this.connection.cluster('NODES', (err, data) => {
          if (err) {
            this.emit('warning', 'Cluster mode failed to retrieve nodes');
            return onReady();
          }
          // Find the master node
          if (typeof data === 'string' && data) {
            data = data.split('\n');
          }
          if (!(data instanceof Array) || data.length === 0) {
            this.emit('warning', 'Cluster mode failed to retrieve nodes');
            return onReady();
          }
          let masterIp, masterPort;
          for (let i = 0, len = data.length; i < len; i++) {
            try {
              let node = data[i];
              if (typeof node !== 'string') continue;
              if (!node) continue;
              // The node is: {hash ip:port who keys ..}, we need ip:port
              let tmp = node.split(' '),
                ip = tmp[1],
                port,
                kind = tmp[2];
              if (!ip) continue;
              if (!kind || kind.indexOf('master') === -1) continue;
              let htmp = ip.split(':');
              ip = htmp[0];
              port = htmp[1];
              if (!port || !ip) continue;
              masterIp = ip;
              masterPort = port;
              break;
            } catch (e) {
            }
          }
          if (!masterIp || !masterPort) {
            this.emit('warning', 'Cluster mode failed to retrieve master node');
            return onReady();
          }
          this.#config.port = parseInt(masterPort);
          this.#config.host = masterIp;
          this.connection.quit();
          this.emit('info', `Connected to master: ${masterIp}:${masterPort}`);
          this.connect(done, true);
        });
      });
    }


    /**
     * Disconnects from redis
     * */
    disconnect() {
      try {
        this.connection.disconnect();
      } catch (e) {
      }
      if (this.#ping) {
        clearInterval(this.#ping);
      }
      this.emit('close');
    }

    /**
     * Starts in subscriber mode and listens for incoming messages.
     * */
    startSubscriber() {
      if (this.#subscriberStarted) return;
      this.#subscriberStarted = true;
      this.connection.on('message', this.handleMessage.bind(this));
    }

    /**
     * Handles any incoming messages from the subscribed connection.
     * */
    handleMessage(channel, msg) {
      if (!this.#subscribers) return;
      if (typeof this.#subscribers[channel] === 'undefined' || this.#subscribers[channel].length === 0) return;
      let fns = this.#subscribers[channel];
      if (msg.charAt(0) === '{' || msg.charAt(0) === "[") {
        try {
          msg = JSON.parse(msg);
        } catch (e) {
        }
      } else if (msg === 'true' || msg === 'false') {
        msg = (msg === 'true');
      }
      this.emit('subscribe.message', channel, msg);
      for (let i = 0; i < fns.length; i++) {
        try {
          fns[i](msg);
        } catch (e) {
          console.error('Thorin.redis: subscriber encountered an error.', fns[i]);
          console.trace(e);
        }
      }
    }

    /**
     * Tries to re-subscribe to the previously subscribed channels.
     * */
    reSubscribe() {
      if (!this.#subscribers) return;
      let channels = Object.keys(this.#subscribers);
      this.startSubscriber();
      for (let i = 0; i < channels.length; i++) {
        try {
          this.connection.subscribe(channels[i]);
        } catch (e) {
        }
      }
    }

    /**
     * Handles the internal channel callback listing.
     * */
    handleSubscribe(channel, fn) {
      if (!this.#subscribers) this.#subscribers = {};
      if (!this.#subscribers[channel]) {
        this.connection.subscribe(channel);
        this.#subscribers[channel] = [];
        this.emit('subscribe.channel', channel);
      }
      this.#subscribers[channel].push(fn);
      return this;
    }

    /**
     * Handles the internal unsubscription.
     * */
    handleUnsubscribe(channel, _fn) {
      if (!this.#subscribers || !this.#subscribers[channel]) return;
      if (typeof _fn !== 'function') {
        delete this.#subscribers[channel];
      } else {
        for (let i = 0; i < this.#subscribers[channel].length; i++) {
          if (this.#subscribers[channel] === _fn) {
            this.#subscribers.splice(i, 1);
            break;
          }
        }
        if (this.#subscribers[channel].length === 0) {
          delete this.#subscribers[channel];
        }
      }
      this.emit('unsubscribe', channel);
      if (Object.keys(this.#subscribers).length === 0) {  // no more.
        this.connection.unsubscribe();
      }
    }

    /**
     * Regularly sends PING events to the server so that the connection remains open.
     * */
    startPing() {
      this.#ping = setInterval(() => {
        if (!this.connected) return;
        this.connection.ping();
      }, PING_TIMER);
    }
  }

  return RedisConnection;
}
