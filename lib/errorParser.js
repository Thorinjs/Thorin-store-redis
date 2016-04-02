'use strict';
/**
 * Created by Adrian on 02-Apr-16.
 */

/*
* Checks if the given error contains any kind of sequelize information.
* If it does, we will mutate it so that the error ns is SQL
* */
function parseError(e) {
  if(e.ns === 'STORE.REDIS') return true;
  if(e.code && e.code.indexOf('REDIS') === 0) {
    e.ns = 'STORE.REDIS';
    return true;
  }
}

module.exports = parseError;