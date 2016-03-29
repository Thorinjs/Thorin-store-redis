'use strict';
const redis = require('redis'),
  EventEmitter = require('events').EventEmitter;
/**
 * Created by Adrian on 29-Mar-16.
 */
const PING_TIMER = 50 * 1000;
module.exports = class RedisConnection extends EventEmitter {

  constructor(name, config) {
    super();
    this.name = name;
    this.config = config;
    this.connection = null;
    this.connected = false;
    this.subscriberStarted = false;
    this.subscribers = null;
  }

  /*
   * Connects to redis.
   * */
  connect(done) {
    if (this.config.password) {
      this.config.options['auth_pass'] = this.config.password;
    }
    redis.debug_mode = false;
    this.connection = redis.createClient(this.config.port, this.config.host, this.config.options);
    let isDoneCalled = false,
      isDisconnected = false,
      wasConnected = false;
    this.connection.on('error', (e) => {
      this.connected = false;
      if (!isDisconnected) {
        isDisconnected = true;
        if (!isDoneCalled) {
          this.emit('disconnect', e);
        }
      }
      if (!isDoneCalled) {
        isDoneCalled = true;
        return done(e);
      }
    });
    this.connection.on('ready', () => {
      this.connected = true;
      if (isDisconnected) {
        isDisconnected = false;
      }
      if (wasConnected) {
        this.emit('reconnect');
      } else {
        wasConnected = true;
      }
      if (!isDoneCalled) {
        isDoneCalled = true;
        return done();
      }
    });
    console.log("TODO");
  }

  /*
   * Disconnects from redis
   * */
  disconnect() {
    try {
      this.connection.end();
    } catch (e) {
    }
    if(this._ping) {
      clearInterval(this._ping);
    }
  }

  /*
   * Starts in subscriber mode and listens for incoming messages.
   * */
  startSubscriber() {
    if(this.subscriberStarted) return;
    this.subscriberStarted = true;
    this.connection.on('message', this.handleMessage.bind(this));
  }

  /*
  * Handles any incoming messages from the subscribed connection.
  * */
  handleMessage(channel, msg) {
    if(typeof this.subscribers[channel] === 'undefined' || this.subscribers[channel].length === 0) return;
    let fns = this.subscribers[channel];
    if(msg.charAt(0) === '{' || msg.charAt(0) === "[") {
      try {
        msg = JSON.parse(msg);
      } catch(e) {}
    } else if(msg === 'true' || msg === 'false') {
      msg = (msg === 'true');
    }
    for(let i=0; i < fns.length; i++) {
      try {
        fns[i](msg);
      } catch(e) {
        console.error('Thorin.redis: subscriber encountered an error.', fns[i]);
        console.debug(e);
      }
    }
  }

  /*
   * Handles the internal channel callback listing.
   * */
  handleSubscribe(channel, fn) {
    if(this.subscribers == null) {
      this.subscribers = {};
    }
    if(!this.subscribers[channel]) {
      this.connection.subscribe(channel);
      this.subscribers[channel] = [];
    }
    this.subscribers[channel].push(fn);
    return this;
  }

  /*
  * Handles the internal unsubscription.
  * */
  handleUnsubscribe(channel, _fn) {
    if(!this.subscribers || !this.subscribers[channel]) return;
    if(typeof _fn !== 'function') {
      delete this.subscribers[channel];
    } else {
      for(let i=0; i < this.subscribers[channel].length; i++) {
        if(this.subscribers[channel] == _fn) {
          this.subscribers.splice(i, 1);
          break;
        }
      }
      if(this.subscribers[channel].length === 0) {
        delete this.subscribers[channel];
      }
    }
    if(Object.keys(this.subscribers).length === 0) {  // no more.
      this.connection.unsubscribe();
    }
  }

  /*
  * Regulary sends PING events to the server so that the connection remains open.
  * */
  startPing() {
    this._ping = setInterval(() => {
      if(!this.connected) return;
      this.connection.ping();
    }, PING_TIMER);
  }
};