'use strict';

const wkx = require('wkx');
const _ = require('lodash');
const moment = require('moment-timezone');
const inherits = require('../../utils/inherits');

module.exports = BaseTypes => {
  BaseTypes.ABSTRACT.prototype.dialectTypes = 'https://clickhouse.yandex/docs/en/data_types';

  BaseTypes.DATE.types.clickhouse = ['DateTime'];
  BaseTypes.STRING.types.clickhouse = ['String'];
  BaseTypes.CHAR.types.clickhouse = ['String'];
  BaseTypes.TEXT.types.clickhouse = ['String'];
  BaseTypes.TINYINT.types.clickhouse = ['Int8'];
  BaseTypes.SMALLINT.types.clickhouse = ['Int16'];
  BaseTypes.MEDIUMINT.types.clickhouse = ['Int32'];
  BaseTypes.INTEGER.types.clickhouse = ['Int32'];
  BaseTypes.BIGINT.types.clickhouse = ['Int64'];
  BaseTypes.FLOAT.types.clickhouse = ['Float32'];
  BaseTypes.TIME.types.clickhouse = false;
  BaseTypes.DATEONLY.types.clickhouse = ['Date'];
  BaseTypes.BOOLEAN.types.clickhouse = ['Int8'];
  BaseTypes.BLOB.types.clickhouse = ['String'];
  BaseTypes.DECIMAL.types.clickhouse = ['Decimal'];
  BaseTypes.UUID.types.clickhouse = true;
  BaseTypes.ENUM.types.clickhouse = true;
  BaseTypes.REAL.types.clickhouse = false;
  BaseTypes.DOUBLE.types.clickhouse = false;
  BaseTypes.GEOMETRY.types.clickhouse = false;
  BaseTypes.JSON.types.clickhouse = ['String'];
	BaseTypes.ARRAY.types.clickhouse = ['Array'];
	
	const StringDefaultFn = () => '';
	const NumberDefaultFn = () => 0;
	
	function STRING(length, binary) {
		if (!(this instanceof STRING)) return new STRING(length, binary);
		BaseTypes.STRING.apply(this, arguments);
	}
	inherits(STRING, BaseTypes.STRING);
	
	STRING.prototype.getDefaultValue = StringDefaultFn;
	STRING.prototype.toSql = function toSql() {
		return 'String';
	};
	BaseTypes.STRING.types.clickhouse = {
		oids: [1043],
		array_oids: [1015]
	};
	
	function CHAR(length, binary) {
		if (!(this instanceof CHAR)) return new CHAR(length, binary);
		BaseTypes.CHAR.apply(this, arguments);
	}
	inherits(CHAR, BaseTypes.CHAR);
	
	CHAR.prototype.toSql = function toSql() {
		return 'FixedString(255)';
	};
	CHAR.prototype.getDefaultValue = StringDefaultFn;
	
	function TEXT(length, binary) {
		if (!(this instanceof TEXT)) return new TEXT(length, binary);
		BaseTypes.TEXT.apply(this, arguments);
	}
	inherits(TEXT, BaseTypes.TEXT);
	
	TEXT.prototype.toSql = function toSql() {
		return BaseTypes.TEXT.prototype.toSql.call(this);
	};
	TEXT.prototype.getDefaultValue = StringDefaultFn;
	
  function BLOB(length) {
    if (!(this instanceof BLOB)) return new BLOB(length);
    BaseTypes.BLOB.apply(this, arguments);
  }
  inherits(BLOB, BaseTypes.BLOB);
	
	BLOB.prototype.toSql = function toSql() {
		return 'String';
	};
	BLOB.prototype._stringify = function _stringify(blob, options) {
		return blob ? JSON.stringify(blob) : `''`;
	};
	BLOB.prototype.getDefaultValue = StringDefaultFn;
	
	
	function TINYINT(length) {
		if (!(this instanceof TINYINT)) return new TINYINT(length);
		BaseTypes.TINYINT.apply(this, arguments);
	}
	inherits(TINYINT, BaseTypes.TINYINT);
	
	TINYINT.prototype.toSql = function toSql() {
		return 'Int8';
	};
	TINYINT.prototype.getDefaultValue = () => 0;
	
	
	function SMALLINT(length) {
		if (!(this instanceof SMALLINT)) return new SMALLINT(length);
		BaseTypes.SMALLINT.apply(this, arguments);
	}
	inherits(SMALLINT, BaseTypes.SMALLINT);
	
	SMALLINT.prototype.toSql = function toSql() {
		return 'Int16';
	};
	SMALLINT.prototype.getDefaultValue = NumberDefaultFn;
	
	function INTEGER(length) {
		if (!(this instanceof INTEGER)) return new INTEGER(length);
		BaseTypes.INTEGER.apply(this, arguments);
	}
	inherits(INTEGER, BaseTypes.INTEGER);
	
	INTEGER.prototype.toSql = function toSql() {
		return 'Int32';
	};
	
	INTEGER.prototype.getDefaultValue = NumberDefaultFn;
	
	
	function BOOLEAN(length) {
		if (!(this instanceof BOOLEAN)) return new BOOLEAN(length);
		BaseTypes.BOOLEAN.apply(this, arguments);
	}
	inherits(BOOLEAN, BaseTypes.BOOLEAN);
	
	BOOLEAN.prototype.toSql = function toSql() {
		return 'Int8';
	};
	BOOLEAN.prototype.getDefaultValue = NumberDefaultFn;
	BOOLEAN.prototype._stringify = function _stringify(bool, options) {
		return bool === true ? 1 : 0;
	};
	
	function MEDIUMINT(length) {
		if (!(this instanceof MEDIUMINT)) return new MEDIUMINT(length);
		BaseTypes.MEDIUMINT.apply(this, arguments);
	}
	inherits(MEDIUMINT, BaseTypes.MEDIUMINT);
	
	MEDIUMINT.prototype.toSql = function toSql() {
		return 'Int32';
	};
	MEDIUMINT.prototype.getDefaultValue = NumberDefaultFn;
	

  function DECIMAL(precision, scale) {
    if (!(this instanceof DECIMAL)) return new DECIMAL(precision, scale);
    BaseTypes.DECIMAL.apply(this, arguments);
  }
  inherits(DECIMAL, BaseTypes.DECIMAL);

  DECIMAL.prototype.toSql = function toSql() {
    let definition = BaseTypes.DECIMAL.prototype.toSql.apply(this);

    if (this._unsigned) {
	    definition = `U${definition}`;
    }

    return definition;
  };
	DECIMAL.prototype.getDefaultValue = NumberDefaultFn;

  function DATE(length) {
    if (!(this instanceof DATE)) return new DATE(length);
    BaseTypes.DATE.apply(this, arguments);
  }
  inherits(DATE, BaseTypes.DATE);

  DATE.prototype.toSql = function toSql() {
    return 'DateTime';
  };

  DATE.prototype._stringify = function _stringify(date, options) {
    date = BaseTypes.DATE.prototype._applyTimezone(date, options);

    return date.format('YYYY-MM-DD HH:mm:ss');
  };

  DATE.parse = function parse(value, options) {
    value = value.string();

    if (value === null) {
      return value;
    }

    if (moment.tz.zone(options.timezone)) {
      value = moment.tz(value, options.timezone).toDate();
    } else {
      value = new Date(value + ' ' + options.timezone);
    }

    return value;
  };
	DATE.prototype.getDefaultValue = () => '0000-00-00 00:00:00';
  
  function DATEONLY() {
    if (!(this instanceof DATEONLY)) return new DATEONLY();
    BaseTypes.DATEONLY.apply(this, arguments);
  }
  inherits(DATEONLY, BaseTypes.DATEONLY);

  DATEONLY.parse = function parse(value) {
    return value.string();
  };
	DATEONLY.prototype.getDefaultValue = () => '0000-00-00';
	DATEONLY.prototype.toSql = function toSql() {
		return 'Date';
	};
  function UUID() {
    if (!(this instanceof UUID)) return new UUID();
    BaseTypes.UUID.apply(this, arguments);
  }
  inherits(UUID, BaseTypes.UUID);

  UUID.prototype.toSql = function toSql() {
    return 'UUID';
  };
	UUID.prototype.getDefaultValue = () => '00000000-0000-0000-0000-000000000000';

  function ENUM() {
    if (!(this instanceof ENUM)) {
      const obj = Object.create(ENUM.prototype);
      ENUM.apply(obj, arguments);
      return obj;
    }
    BaseTypes.ENUM.apply(this, arguments);
  }
  inherits(ENUM, BaseTypes.ENUM);

  ENUM.prototype.toSql = function toSql(options) {
    return 'Enum8(' + _.map(this.values, (value, i) => `${options.escape(value)} = ${i + 1}`).join(', ') + ')';
  };
	ENUM.prototype.getDefaultValue = NumberDefaultFn
	
	
	function ENUM16() {
		if (!(this instanceof ENUM16)) {
			const obj = Object.create(ENUM16.prototype);
			ENUM.apply(obj, arguments);
			return obj;
		}
		BaseTypes.ENUM16.apply(this, arguments);
	}
	inherits(ENUM16, BaseTypes.ENUM);
	
	ENUM16.prototype.toSql = function toSql(options) {
		return 'Enum16(' + _.map(this.values, (value, i) => `${options.escape(value)} = ${i + 1}`).join(', ') + ')';
	};

  function JSONTYPE() {
    if (!(this instanceof JSONTYPE)) return new JSONTYPE();
    BaseTypes.JSON.apply(this, arguments);
  }
  inherits(JSONTYPE, BaseTypes.JSON);

  JSONTYPE.prototype._stringify = function _stringify(value, options) {
    return options.operation === 'where' && typeof value === 'string' ? value : JSON.stringify(value);
  };
	JSONTYPE.prototype.getDefaultValue = StringDefaultFn;
	
	// function ARRAY(type) {
	//	
	// 	console.log('ARRAY constructor', new Error().stack, type, _.isPlainObject(type))
	// 	const options = _.isPlainObject(type) ? type : {type};
	// 	if (!(this instanceof ARRAY)) return new ARRAY(options);
	// 	this.type = typeof options.type === 'function' ? new options.type() : options.type;
	// }
	// inherits(ARRAY, BaseTypes.ARRAY);
	//
	// ARRAY.prototype.key = ARRAY.key = 'ARRAY';
	// ARRAY.prototype.toSql = function toSql() {
	// 	return this.type.toSql() + '[1]';
	// };
	// ARRAY.prototype.validate = function validate(value) {
	// 	if (!_.isArray(value)) {
	// 		throw new sequelizeErrors.ValidationError(util.format('%j is not a valid array', value));
	// 	}
	//	
	// 	return true;
	// };
	// ARRAY.is = function is(obj, type) {
	// 	return obj instanceof ARRAY && obj.type instanceof type;
	// };
	BaseTypes.ARRAY.prototype.toSql = function toSql() {
		return 'Array(' + this.type.toSql() + ')';
	};
	// BaseTypes.ARRAY.prototype.escape = false;
	BaseTypes.ARRAY.prototype._stringify = function _stringify(values, options) {
		let str = '[' + values.map(value => {
			console.log('!this.type', value, options, this.type)
			if (this.type && this.type.stringify) {
				value = this.type.stringify(value, options);

				if (this.type.escape === false) {
					return value;
				}
			}
			return options.escape(value);
		}, this).join(',') + ']';

		// if (this.type) {
		// 	const Utils = require('../../utils');
		// 	let castKey = this.toSql();
		//
		// 	if (this.type instanceof BaseTypes.ENUM) {
		// 		castKey = Utils.addTicks(
		// 			Utils.generateEnumName(options.field.Model.getTableName(), options.field.fieldName),
		// 			'"'
		// 		) + '[]';
		// 	}
		//
		// 	str += '::' + castKey;
		// }

		return str;
	};
	BaseTypes.ARRAY.prototype.getDefaultValue = () => [];
	
	
  const exports = {
	  DATE,
	  STRING,
	  CHAR,
	  TEXT,
	  TINYINT,
	  SMALLINT,
	  MEDIUMINT,
	  INTEGER,
	  // BIGINT,
	  // FLOAT,
	  // TIME,
	  // DATEONLY,
	  BOOLEAN,
    ENUM,
	  ENUM16,
    DATE,
    DATEONLY,
    UUID,
    // GEOMETRY,
    DECIMAL,
    BLOB,
    JSON: JSONTYPE,
  };

  _.forIn(exports, (DataType, key) => {
    if (!DataType.key) DataType.key = key;
    if (!DataType.extend) {
      DataType.extend = function extend(oldType) {
        return new DataType(oldType.options);
      };
    }
  });

  return exports;
};
