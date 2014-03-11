/**
 * Simple MySQL Pool
 *
 * @author 老雷<leizongmin@gmail.com>
 */

var util = require('util');
var events = require('events');
var mysql = require('mysql');
var async = require('async');
var debug = require('debug')('lei-mysql');

exports = module.exports = MySQLPool;
exports.escape = mysqlEscape;
exports.escapeId = mysqlEscapeId;


/**
 * 创建MySQL连接池
 *
 * @param {Object} options
 *  - {String} host       主机地址
 *  - {Number} port       端口
 *  - {Number} database   数据库名
 *  - {String} user       用户名
 *  - {String} password   密码
 *  - {Number} pool       连接池大小
 */
function MySQLPool (options) {
  options = options || {};
  if (!options.host)        throw new Error('Invalid host.');
  if (!(options.port > 0))  throw new Error('Invalid port.');
  if (!options.database)    throw new Error('Invalid database.');
  if (!options.user)        throw new Error('Invalid user.');
  if (!(options.pool > 0))  throw new Error('Invalid pool number.');
  this._options = options;

  var pool = this._pool = mysql.createPool({
    host:             options.host,
    port:             options.port,
    database:         options.database,
    user:             options.user,
    password:         options.password,
    connectionLimit:  options.pool
  });

  // 中间件
  this._filters = {
    sql:    [],
    result: [],
    error:  []
  };

  // 查询计数
  this._counter = 0;

  debug('Create MySQLPool: pool=%d, host=%s:%s', options.pool, options.host, options.port);
}

// 继承EventEmitter
util.inherits(MySQLPool, events.EventEmitter);

/**
 * 注册中间件
 *
 * @param {String} method 可取值：update, insert, delete, sql, result, error
 * @param {Function} fn
 */
MySQLPool.prototype.use = function (method, fn) {
  method = method.trim().toLowerCase();
  if (method === 'result') {
    this._filters.result.push(fn);
  } else if (method === 'error') {
    this._filters.error.push(fn);
  } else {
    this._filters.sql.push([method, fn]);
  }
  debug('Use [%s] %s', method, fn);
};

// 执行 result 中间件
// 函数参数： function (sql, data, next) {}
// 返回格式： next(null, data);
//     通过返回 data 参数来覆盖返回结果（必须）
MySQLPool.prototype._runFilterResult = function (sql, data, callback) {
  var list = this._filters.result;
  if (!(list && list.length > 0)) {
    return callback(null, data);
  }
  var fnList = [function (next) {
    next(null, data);
  }];
  list.forEach(function (fn) {
    fnList.push(function (data, next) {
      fn(sql, data, next);
    });
  });
  async.waterfall(fnList, callback);
};

// 执行 error 中间件
// 函数参数： function (sql, err, next) {}
// 返回格式： next()
MySQLPool.prototype._runFilterError = function (sql, err, callback) {
  var list = this._filters.error;
  if (!(list && list.length > 0)) {
    return callback(err);
  }
  var fnList = list.map(function (fn) {
    return function (next) {
      fn(sql, err, next);
    };
  });
  async.series(fnList, function (err2) {
    callback(err2 || err, err);
  });
};

// 执行 sql 中间件
// 函数参数： function (sql, next) {}
// 返回格式： next(null, sql)
//     通过返回 sql 参数来覆盖SQL语句（必须）
//     如果返回的sql不是string，则表示直接返回数据，不再查询数据库，
//     也不会触发后面的result中间件
MySQLPool.prototype._runFilterSql = function (sql, callback) {
  var list = this._filters.sql;
  if (!(list && list.length > 0)) {
    return callback(null, sql);
  }
  var fnList = [function (next) {
    next(null, sql);
  }];
  list.forEach(function (item) {
    var m = item[0];
    var fn = item[1];
    fnList.push(function (sql, next) {
      if (typeof sql === 'string') {
        if (m === 'sql' || m === getSqlMethod(sql)) {
          return fn(sql, next);
        }
      }
      next(null, sql);
    });
  });
  async.waterfall(fnList, callback);
};

/**
 * 执行查询
 *
 * @param {String} sql
 * @param {Function} callback
 */
MySQLPool.prototype.query = function (sql, callback) {
  var me = this;
  me._counter++;
  debug('Query [#%s]: %s', me._counter, sql);

  // SQL预处理
  me._runFilterSql(sql, function (err, sql) {
    if (err) {
      me._runFilterError(sql, err, callback);
    } else if (typeof sql !== 'string') {
      callback(err, sql);
    } else {
      me._pool.getConnection(function (err, conn) {
        conn.query(sql, function (err, data) {
          conn.release();
          if (err) {
            debug('Query failure [#%s]: %s', me._counter, err);
            me._runFilterError(sql, err, callback);
          } else {
            debug('Query success [#%s]', me._counter);
            me._runFilterResult(sql, data, callback);
          }
        });
      });
    }
  });
};

/**
 * SQL值串转义
 *
 * @param {String} value
 * @return {String}
 */
MySQLPool.prototype.escape = mysqlEscape;

/**
 * SQL名称转义
 *
 * @param {String} name
 * @return {String}
 */
MySQLPool.prototype.escapeId = mysqlEscapeId;

/**
 * 当前时间戳
 *
 * @return {Number}
 */
MySQLPool.prototype.timestamp = function () {
  return parseInt(Date.now() / 1000, 10);
};

/**
 * 插入数据
 *
 * @param {String} table
 * @param {Object|Array} data
 * @param {Function} callback
 */
MySQLPool.prototype.insert = function (table, data, callback) {
  var me = this;
  if (!Array.isArray(data)) data = [data];
  if (!(data[0] && typeof data[0] === 'object')) {
    return callback(new Error('Bad data format.'));
  }

  // 取完整的键名
  var fields = {};
  data.forEach(function (item) {
    for (var i in item) {
      if (!fields[i]) fields[i] = true;
    }
  });
  fields = Object.keys(fields);

  // 生成数据列表
  var values = [];
  data.forEach(function (item) {
    var line = [];
    fields.forEach(function (f) {
      line.push(item[f] || '');
    });
    values.push(line);
  });

  // 生成SQL
  var fields = fields.map(function (f) {
    return me.escapeId(f);
  });
  var values = values.map(function (line) {
                 return '(' + line.map(function (v) {
                    return me.escape(v);
                 })
                 .join(',') + ')';
               })
               .join(',\n');
  var sql = 'INSERT INTO ' + this.escapeId(table) + '(' + fields + ') VALUES\n' + values;

  me.query(sql, callback);
};

/**
 * 更新数据库
 *
 * @param {String} table
 * @param {String|Array|Object} where
 * @param {String|Object} data
 * @param {String} tail 可选
 * @param {Function} callback
 */
MySQLPool.prototype.update = function (table, where, data, tail, callback) {
  if (typeof tail === 'function') {
    callback = tail;
    tail = '';
  }

  where = parseCondition(where);

  if (data && typeof data === 'object') {
    var set = [];
    for (var i in data) {
      var v = data[i];
      var n = this.escapeId(i);
      if (Array.isArray(v)) {
        if (v[0] === '$incr') {
          set.push(n + '=' + n + '+(' + this.escape(v[1]) + ')');
        } else {
          return callback(new Error('Not support operator "' + v[0] + '" for parameter "data".'));
        }
      } else {
        set.push(n + '=' + this.escape(v));
      }
    }
    set = set.join(',');
  } else if (typeof data === 'string') {
    var set = data.trim();
  } else {
    return callback(new Error('Parameter "data" must be an object or string.'));
  }

  var sql = 'UPDATE ' + this.escapeId(table) + ' SET ' + set + ' WHERE ' + where + ' ' + tail;

  this.query(sql, callback);
};

/**
 * 删除
 *
 * @param {String} table
 * @param {String|Array|Object} where
 * @param {String} tail 可选
 * @param {Function} callback
 */
MySQLPool.prototype.delete = function (table, where, tail, callback) {
  if (typeof tail === 'function') {
    callback = tail;
    tail = '';
  }

  where = parseCondition(where);

  var sql = 'DELETE FROM ' + this.escapeId(table) + ' WHERE ' + where + ' ' + tail;

  this.query(sql, callback);
};

/**
 * 查询
 *
 * @param {String} table
 * @param {String|Array|Object} where
 * @param {Object} options
 *   - {String|Array} fields
 *   - {String} tail
 * @param {Function} callback
 */
MySQLPool.prototype.find = function (table, where, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }
  options.fields = options.fields || '*';
  options.tail = options.tail || '';

  if (Array.isArray(options.fields)) {
    options.fields = options.fields.map(function (item) {
      return '`' + item + '`';
    }).join(', ');
  }

  where = parseCondition(where);

  var sql = 'SELECT ' + options.fields + ' FROM ' + this.escapeId(table) + ' WHERE ' + where + ' ' + options.tail;
  sql = sql.trim();

  this.query(sql, callback);
};

/**
 * 仅查询一条
 *
 * @param {String} table
 * @param {String|Array|Object} where
 * @param {Object} options
 *   - {String|Array} fields
 *   - {String} tail
 * @param {Function} callback
 */
MySQLPool.prototype.findOne = function (table, where, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }
  options.fields = options.fields || '*';
  options.tail = options.tail || '';

  if (options.tail.toLowerCase().indexOf('limit ') === -1) {
    options.tail += ' LIMIT 1';
  }

  this.find(table, where, options, function (err, list) {
    if (err) return callback(err);
    callback(null, list && list[0]);
  });
};

/**
 * 查询数量
 *
 * @param {String} table
 * @param {String|Array|Object} where
 * @param {Function} callback
 */
MySQLPool.prototype.count = function (table, where, callback) {
  this.findOne(table, where, {fields: 'COUNT(*) AS `c`'}, function (err, info) {
    callback(err, info ? info.c : 0);
  });
};

/**
 * 创建表
 *
 * @param {String} table
 * @param {Object} fields
 * @param {Array} indexes 可选
 * @param {Function} callback
 */
MySQLPool.prototype.createTable = function (table, fields, indexes, callback) {
  if (typeof indexes === 'function') {
    callback = indexes;
    indexes = [];
  }

  var lines = [];
  for (var i in fields) {
    lines.push(parseCreateTableField(i, fields[i]));
  }
  lines = lines.concat(indexes.map(parseCreateTableIndex));
  var sql = 'CREATE TABLE ' + this.escapeId(table) + '(\n' + lines.join(',\n') + '\n)';
  this.query(sql, callback);
};

/**
 * 更新表
 *
 * @param {String} table
 * @param {Object} fields
 * @param {Array} indexes 可选
 * @param {Function} callback
 */
MySQLPool.prototype.updateTable = function (table, fields, indexes, callback) {
  var me = this;
  if (typeof indexes === 'function') {
    callback = indexes;
    indexes = [];
  }

  // 需要先查询出表结构，然后计算出有变动的地方
  me.showFields(table, function (err, oldFields) {
    if (err) return callback(err);
    me.showIndexes(table, function (err, oldIndexes) {
      if (err) return callback(err);

      var lines = [];

      // 生成修改键的SQL
      var diffFields = [];
      for (var i in fields) {
        if (i in oldFields) {
          diffFields.push(['CHANGE', i, fields[i]]);
        } else {
          diffFields.push(['ADD', i, fields[i]]);
        }
      }
      for (var i in oldFields) {
        if (!(i in fields)) {
          diffFields.push(['DROP', i]);
        }
      }
      diffFields.forEach(function (item) {
        lines.push(parseUpdateTableField(item[0], item[1], item[2]));
      });

      // 生产修改索引的SQL
      // 1、将oldIndexes中没有出现在currIndexes的 DROP
      // 2、将currIndexes中没有出现在oldIndexes的 ADD
      // 3、都存在的不作修改
      var diffIndexes = [];
      var currIndexes = [];
      indexes.forEach(function (item) {
        currIndexes = currIndexes.concat(makeUpdateTableIndex(item));
      });
      function indexIsExists (list, a) {
        for (var i = 0; i < list.length; i++) {
          var b = list[i];
          if (fieldsIsEqual(a.fields, b.fields) && a.primary === b.primary &&
              a.unique === b.unique && a.fullText === b.fullText) {
            return true;
          }
        }
        return false;
      }
      oldIndexes.forEach(function (v) {
        if (!indexIsExists(currIndexes, v)) {
          diffIndexes.push(['DROP', v]);
        }
      });
      currIndexes.forEach(function (v) {
        if (!indexIsExists(oldIndexes, v)) {
          diffIndexes.push(['ADD', v]);
        }
      });
      lines = lines.concat(diffIndexes.map(function (item) {
        return parseUpdateTableIndex(item[0], item[1]);
      }));

      var sql = 'ALTER TABLE ' + me.escapeId(table) + '\n' + lines.join(',\n');
      me.query(sql, callback);
    })
  });
};

/**
 * 删除表
 *
 * @param {String} table
 * @param {Function} callback
 */
MySQLPool.prototype.dropTable = function (table, callback) {
  this.query('DROP TABLE IF EXISTS ' + this.escapeId(table), callback);
};

/**
 * 查询字段列表
 *
 * @param {String} table
 * @param {Function} callback
 */
MySQLPool.prototype.showFields = function (table, callback) {
  this.query('SHOW FIELDS FROM ' + this.escapeId(table), function (err, list) {
    if (err) return callback(err);

    var ret = {};
    list.forEach(function (a) {
      var b = {};
      for (var i in a) {
        b[i.toLowerCase()] = a[i];
      }
      var c = {};
      var t = b.type.match(/([^()]+)\(([^()]+)\)/);
      if (t) {
        c.type = t[1];
        c.size = t[2];
      } else {
        c.type = b.type;
        c.size = '';
      }
      c.type = c.type.toUpperCase();
      c.null = (b.null.toUpperCase() === 'YES' ? true : false);
      c.default = b.default;
      c.autoIncrement = (b.extra.toLowerCase().indexOf('auto_increment') !== -1 ? true : false);
      var k = b.key.toUpperCase();
      c.primary = (k === 'PRI' ? true : false);
      c.unique = (k === 'UNI' ? true : false);
      c.index = (k === 'MUL' ? true : false);
      ret[b.field] = c;
    });

    callback(null, ret);
  });
};

/**
 * 查询索引列表
 *
 * @param {String} table
 * @param {Function} callback
 */
MySQLPool.prototype.showIndexes = function (table, callback) {
  this.query('SHOW INDEXES FROM ' + this.escapeId(table), function (err, list) {
    if (err) return callback(err);

    var indexes = {};
    list.forEach(function (item) {
      var a = {};
      Object.keys(item).forEach(function (k) {
        a[convertName(k)] = item[k];
      });
      var b = {};
      b.primary = (a.keyName.toUpperCase() === 'PRIMARY' ? true : false);
      b.unique = (!b.primary && a.nonUnique === 0 ? true : false);
      b.fullText = (!b.unique && a.indexType.toUpperCase() === 'FULLTEXT' ? true : false);

      if (a.keyName in indexes) {
        indexes[a.keyName].fields.push(a.columnName);
      } else {
        b.fields = [a.columnName];
        indexes[a.keyName] = b;
      }
    });

    var ret = Object.keys(indexes).map(function (k) {
      var a = indexes[k];
      a.key = k;
      if (a.fields.length === 1) a.fields = a.fields[0];
      return a;
    });

    callback(null, ret);
  });
};

/**
 * SQL值转义
 *
 * @param {Mixed} value
 * @return {String}
 */
function mysqlEscape (value) {
  return mysql.escape(value);
}

/**
 * SQL名称转义
 *
 * @param {String} name
 * @return {String}
 */
function mysqlEscapeId (name) {
  return mysql.escapeId(name);
};

/**
 * 解析Where条件
 *
 * @param {String|Array|Object} w
 * @return {String}
 */
function parseCondition (w) {
  if (typeof w === 'string') return w;
  if (Array.isArray(w)) return parseArrayCondition(w);
  if (w && typeof w === 'object') return parseObjectCondition(w);
  if (w === undefined || w === null || w === false) return false;
  if (w === true) return true;
  return w;
}

function parseArrayCondition (w) {
  if (!Array.isArray(w)) return mysqlEscape(w);

  switch (w[0]) {
    case '$and':
    case '$or':
      var c = w[0].substr(1).toUpperCase();
      var ret = w.slice(1).map(parseArrayCondition).join(' ' + c + ' ');
      break;
    case '$not':
      var ret = 'NOT ' + parseArrayCondition(w[1]);
      break;
    default:
      if (w.length === 0) var ret = false;
      else if (w.length === 1) var ret = w;
      else if (w.length === 2) var ret = '`' + w[0] + '`=' + parseArrayCondition(w[1]);
      else var ret = '`' + w[0] + '` ' + w[1] + ' ' + parseArrayCondition(w[2]);
  }

  return ' (' + ret + ') ';
}

function parseObjectCondition (w) {
  var list = [];
  for (var i in w) {
    list.push('`' + i + '`=' + mysqlEscape(w[i]));
  }
  return list.join(' AND ');
}

/**
 * 生成创建表属性的SQL
 *
 * @param {String} name
 * @param {String|Object} data
 *   - {String} type
 *   - {String} size
 *   - {String} charset
 *   - {Boolean} null
 *   - {null|String} default
 *   - {Boolean} autoIncrement
 * @return {String}
 */
function parseCreateTableField (name, data) {
  var line = [mysqlEscapeId(name)];
  if (typeof data === 'string') {
    line.push(data.toUpperCase());
  } else {
    data.type = data.type.toUpperCase();
    if (data.size) {
      line.push(data.type + '(' + data.size + ')');
    } else {
      line.push(data.type);
    }
    if (data.charset) line.push('CHARACTER SET ' + data.charset);
    if (data.null) {
      line.push('NULL');
    } else {
      line.push('NOT NULL');
    }
    if ('default' in data) {
      line.push('DEFAULT ' + (data.default === null ? 'NULL' : mysqlEscape(data.default)));
    }
    if (data.autoIncrement) line.push('AUTO_INCREMENT');
  }
  return line.join(' ');
}

/**
 * 生成修改表属性的SQL
 *
 * @param {String} op
 * @param {String} name
 * @param {String|Object} data
 *   - {String} type
 *   - {String} size
 *   - {String} charset
 *   - {Boolean} null
 *   - {null|String} default
 *   - {Boolean} autoIncrement
 * @return {String}
 */
function parseUpdateTableField (op, name, data) {
  if (op === 'DROP') return 'DROP ' + mysqlEscapeId(name);
  var sql = parseCreateTableField(name, data);
  if (op === 'CHANGE') return 'CHANGE ' + mysqlEscapeId(name) + ' ' + sql;
  else return 'ADD ' + sql;
}

/**
 * 生成创建表索引的SQL
 *
 * @param {String|Array|Object} data
 *   - {String|Array} fields
 *   - {Boolean} primary
 *   - {Boolean} unique
 *   - {Boolean} fullText
 * @return {String}
 */
function parseCreateTableIndex (data) {
  if (typeof data === 'string') {
    return 'KEY ' + mysqlEscapeId(data) + '(' + mysqlEscapeId(data) + ')';
  } else if (Array.isArray(data)) {
    var k = data.join('_');
    var f = data.map(mysqlEscapeId).join(', ');
    return 'KEY ' + mysqlEscapeId(k) + '(' + f + ')';
  } else {
    if (Array.isArray(data.fields)) {
      var k = data.fields.join('_');
      var f = data.fields.map(mysqlEscapeId).join(', ');
    } else {
      var k = data.fields;
      var f = mysqlEscapeId(data.fields);
    }
    var s = k + '(' + f + ')';
    if (data.primary) return 'PRIMARY KEY ' + s;
    if (data.unique) return 'UNIQUE KEY ' + s;
    if (data.fullText) return 'FULLTEXT KEY ' + s;
    return 'KEY ' + s;
  }
}

/**
 * 生成修改表索引的数据结构
 *
 * @param {String|Array|Object} data
 *   - {String|Array} fields
 *   - {Boolean} primary
 *   - {Boolean} unique
 *   - {Boolean} fullText
 * @return {Object}
 */
function makeUpdateTableIndex (data) {
  if (typeof data === 'string' || Array.isArray(data)) {
    data = {fields: data};
  }

  data.primary = !!data.primary;
  data.unique = !!data.unique;
  data.fullText = !!data.fullText;
  return data;
}

/**
 * 生成修改表索引的SQL
 *
 * @param {String} op
 * @param {String|Array} data
 *   - {String|Array} fields
 *   - {Boolean} primary
 *   - {Boolean} unique
 *   - {Boolean} fullText
 * @return {String}
 */
function parseUpdateTableIndex (op, data) {
  if (op === 'DROP') {
    if (data.primary) {
      return 'DROP PRIMARY KEY';
    } else {
      return 'DROP INDEX ' + mysqlEscapeId(data.key);
    }
  } else if (op === 'ADD') {
    if (Array.isArray(data.fields)) {
      var k = data.fields.map(mysqlEscapeId).join(', ');
    } else {
      var k = mysqlEscapeId(data.fields);
    }
    k = '(' + k + ')';
    if (data.primary) return 'ADD PRIMARY KEY ' + k;
    if (data.unique) return 'ADD UNIQUE ' + k;
    if (data.fullText) return 'ADD FULLTEXT ' + k;
    return 'ADD INDEX ' + k;
  }
}

/**
 * 判断fields字段是否相等
 *
 * @param {String|Array} a
 * @param {String|Array} b
 * @return {Boolean}
 */
function fieldsIsEqual (a, b) {
  if (a === b) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  } else {
    return false;
  }
}

/**
 * 将名称转化成驼峰命名方式
 *
 * @param {String} n
 * @return {String}
 */
function convertName (n) {
  var a = n.split('_');
  if (a.length > 1) {
    n = a[0] + a.slice(1).map(function (v) {
      return v[0].toUpperCase() + v.slice(1).toLowerCase();
    }).join('');
  } else {
    n = a;
  }
  return n[0].toLowerCase() + n.slice(1);
}

/**
 * 将arguments转为数组
 *
 * @param {Object} args
 * @return {Array}
 */
function toArray (args) {
  return Array.prototype.slice.call(args, 0);
}

/**
 * 获取当前SQL查询语句的方法
 *
 * @param {String} sql
 * @return {String}
 */
function getSqlMethod (sql) {
  sql = sql.trim();
  var i = sql.indexOf(' ');
  if (i === -1) return 'all';
  return sql.slice(0, i).toLowerCase();
}
