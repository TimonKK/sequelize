'use strict';

const _ = require('lodash');
const AbstractConnectionManager = require('../abstract/connection-manager');
const SequelizeErrors = require('../../errors');
const Utils = require('../../utils');
const DataTypes = require('../../data-types').clickhouse;
const momentTz = require('moment-timezone');
const debug = Utils.getLogger().debugContext('connection:clickhouse');
const parserMap = new Map();

/**
 * MySQL Connection Managger
 *
 * Get connections, validate and disconnect them.
 * AbstractConnectionManager pooling use it to handle MySQL specific connections
 * Use https://github.com/sidorares/node-mysql2 to connect with MySQL server
 *
 * @extends AbstractConnectionManager
 * @return Class<ConnectionManager>
 * @private
 */

class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    super(dialect, sequelize);

    this.sequelize = sequelize;
    this.sequelize.config.database = this.sequelize.config.database || 'default';
    this.sequelize.config.username = this.sequelize.config.username || 'default';
    this.sequelize.config.password = this.sequelize.config.password || '';
    this.sequelize.config.port = this.sequelize.config.port || 8123;
    try {
      if (sequelize.config.dialectModulePath) {
        this.lib = require(sequelize.config.dialectModulePath).ClickHouse;
      } else {
        this.lib = require('clickhouse').ClickHouse;
      }
    } catch (err) {
      if (err.code === 'MODULE_NOT_FOUND') {
        throw new Error('Please install clickhouse package manually');
      }
      throw err;
    }

    this.refreshTypeParser(DataTypes);
  }

  // Update parsing when the user has added additional, custom types
  _refreshTypeParser(dataType) {
    for (const type of dataType.types.clickhouse) {
      parserMap.set(type, dataType.parse);
    }
  }

  _clearTypeParser() {
    parserMap.clear();
  }

  static _typecast(field, next) {
    if (parserMap.has(field.type)) {
      return parserMap.get(field.type)(field, this.sequelize.options, next);
    }
    return next();
  }

  /**
   * Connect with Mysql database based on config, Handle any errors in connection
   * Set the pool handlers on connection.error
   * Also set proper timezone once conection is connected
   *
   * @return Promise<Connection>
   * @private
   */
  connect(config) {
    const connectionConfig = {
      url: config.host || config.url,
      port: config.port || this.sequelize.config.port,
      user: config.username,
      flags: '-FOUND_ROWS',
      password: config.password,
      database: config.database,
      timezone: this.sequelize.options.timezone,
      typeCast: ConnectionManager._typecast.bind(this),
      bigNumberStrings: false,
      supportBigNumbers: true,
	    debug: config.debug || this.sequelize.config.debug
    };

    if (config.dialectOptions) {
      for (const key of Object.keys(config.dialectOptions)) {
        connectionConfig[key] = config.dialectOptions[key];
      }
    }

    return new Utils.Promise((resolve, reject) => {
      const connection = new this.lib(connectionConfig);
      resolve(connection);
    })
      .tap (() => { debug('connection acquired'); })
      // .then(connection => {
      //   return new Utils.Promise((resolve, reject) => {
      //   // set timezone for this connection
      //   // but named timezone are not directly supported in clickhouse, so get its offset first
      //     let tzOffset = this.sequelize.options.timezone;
      //     tzOffset = /\//.test(tzOffset) ? momentTz.tz(tzOffset).format('Z') : tzOffset;
      //     console.log('FFFFFFFFFFFFFFFFFFFF', `SET time_zone = '${tzOffset}'`)
      //     connection.query(`SET time_zone = '${tzOffset}'`, err => {
      //     	console.log('GGGGGGGGGGG', err)
      //       if (err) { reject(err); } else { resolve(connection); }
      //     });
      //   });
      // })
      .catch(err => {
        switch (err.code) {
          case 'ECONNREFUSED':
            throw new SequelizeErrors.ConnectionRefusedError(err);
          case 'ER_ACCESS_DENIED_ERROR':
            throw new SequelizeErrors.AccessDeniedError(err);
          case 'ENOTFOUND':
            throw new SequelizeErrors.HostNotFoundError(err);
          case 'EHOSTUNREACH':
            throw new SequelizeErrors.HostNotReachableError(err);
          case 'EINVAL':
            throw new SequelizeErrors.InvalidConnectionError(err);
          default:
            throw new SequelizeErrors.ConnectionError(err);
        }
      });
  }

  disconnect(connection) {
	  return Utils.Promise.resolve();
  }
}

_.extend(ConnectionManager.prototype, AbstractConnectionManager.prototype);

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
