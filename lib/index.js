/**
 * Simple MySQL Pool
 *
 * @author 老雷<leizongmin@gmail.com>
 */

var util = require('util');
var events = require('events');
var mysql = require('mysql');
var debug = require('debug')('lei-mysql');

exports = module.exports = MySQLPool;


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

  debug('Create MySQLPool: pool=%d, host=%s:%s', options.pool, options.host, options.port);
}

// 继承EventEmitter
util.inherits(MySQLPool, events.EventEmitter);

/**
 * 执行查询
 *
 * @param {String} sql
 * @param {Array} params
 * @param {Function} callback
 */
MySQLPool.prototype.query = function () {
  var args = arguments;
  var len = args.length;
  var callback = args[len - 1];

  this._pool.getConnection(function(err, conn) {
    if (err) return callback(err);

    args[len - 1] = function (err) {
      conn.release();
      callback.apply(null, arguments);
    };
    conn.query.apply(conn, args);

    debug('Query: %s', args[0]);
  });
};

/**
 * SQL值串转义
 *
 * @param {String} value
 * @return {String}
 */
MySQLPool.prototype.escape = function (value) {
  return SqlStringEscape(value, false, this._options.timezone);
};

/**
 * SQL名称转义
 *
 * @param {String} name
 * @return {String}
 */
MySQLPool.prototype.escapeName = escapeName;

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
    return me.escapeName(f);
  });
  var values = values.map(function (line) {
                 return '(' + line.map(function (v) {
                    return me.escape(v);
                 })
                 .join(',') + ')';
               })
               .join(',\n');
  var sql = 'INSERT INTO ' + this.escapeName(table) + '(' + fields + ') VALUES\n' + values;

  me.query(sql, callback);
};

/**
 * 更新数据库
 *
 * @param {String} table
 * @param {String|Array|Object} where
 * @param {Object} data
 * @param {String} tail 可选
 * @param {Function} callback
 */
MySQLPool.prototype.update = function (table, where, data, tail, callback) {
  if (typeof tail === 'function') {
    callback = tail;
    tail = '';
  }

  where = parseCondition(where);

  if (!(data && typeof data === 'object')) {
    return callback(new Error('Data must be an object.'));
  }

  var set = [];
  for (var i in data) {
    set.push(this.escapeName(i) + '=' + this.escape(data[i]));
  }
  var sql = 'UPDATE ' + this.escapeName(table) + ' SET ' + set.join(',') + ' WHERE ' + where + ' ' + tail;

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

  var sql = 'DELETE FROM ' + this.escapeName(table) + ' WHERE ' + where + ' ' + tail;

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

  var sql = 'SELECT ' + options.fields + ' FROM ' + this.escapeName(table) + ' WHERE ' + where + ' ' + options.tail;
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
  var sql = 'CREATE TABLE ' + this.escapeName(table) + '(\n' + lines.join(',\n') + '\n)';
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
    me.query('SHOW INDEXES FROM ' + escapeName(table), function (err, oldIndexes) {
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
      oldIndexes = parseUpdateTableOldIndex(oldIndexes);
      var diffIndexes = [];
      var currIndexes = [];
      indexes.forEach(function (item) {
        currIndexes = currIndexes.concat(parseUpdateTableIndex(item));
      });
      console.log(oldIndexes);
      console.log(currIndexes);
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
          diffIndexes.push('DROP', v);
        }
      });
      currIndexes.forEach(function (v) {
        if (!indexIsExists(oldIndexes, v)) {
          diffIndexes.push('ADD', v);
        }
      });
      // 如果主键有变动，则删除原来的主键
      console.log(diffIndexes);

      var sql = 'ALTER TABLE ' + me.escapeName(table) + '\n' + lines.join(',\n');
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
  this.query('DROP TABLE IF EXISTS ' + this.escapeName(table), callback);
};

/**
 * 查询字段列表
 *
 * @param {String} table
 * @param {Function} callback
 */
MySQLPool.prototype.showFields = function (table, callback) {
  this.query('SHOW FIELDS FROM ' + this.escapeName(table), function (err, list) {
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
  this.query('SHOW INDEXES FROM ' + this.escapeName(table), function (err, list) {
    if (err) return callback(err);

    var ret = {};
    list.forEach(function (item) {
      var a = {};
      Object.keys(item).forEach(function (k) {
        a[convertName(k)] = item[k];
      });
      a.primary = (a.keyName.toUpperCase() === 'PRIMARY' ? true : false);
      a.unique = (a.nonUnique === 0 ? true : false);
      a.fullText = (a.indexType.toUpperCase() === 'FULLTEXT' ? true : false);
      if (a.columnName in ret) {
        ret[a.columnName].push(a);
      } else {
        ret[a.columnName] = [a];
      }
    });

    callback(null, ret);
  });
};

/**
 * MySQL字符串转义（取自mysql模块lib/protocol/SqlString.js）
 */
function SqlStringEscape (val, stringifyObjects, timeZone) {
  if (val === undefined || val === null) {
    return 'NULL';
  }

  switch (typeof val) {
    case 'boolean': return (val) ? 'true' : 'false';
    case 'number': return val+'';
  }

  if (val instanceof Date) {
    val = SqlString.dateToString(val, timeZone || "Z");
  }

  if (Buffer.isBuffer(val)) {
    return SqlString.bufferToString(val);
  }

  if (Array.isArray(val)) {
    return SqlString.arrayToList(val, timeZone);
  }

  if (typeof val === 'object') {
    if (stringifyObjects) {
      val = val.toString();
    } else {
      return SqlString.objectToValues(val, timeZone);
    }
  }

  val = val.replace(/[\0\n\r\b\t\\\'\"\x1a]/g, function(s) {
    switch(s) {
      case "\0": return "\\0";
      case "\n": return "\\n";
      case "\r": return "\\r";
      case "\b": return "\\b";
      case "\t": return "\\t";
      case "\x1a": return "\\Z";
      default: return "\\"+s;
    }
  });
  return "'"+val+"'";
}

/**
 * SQL名称转义
 *
 * @param {String} name
 * @return {String}
 */
function escapeName (name) {
  return '`' + name.replace(/\./g, '`.`') + '`';
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
  if (!Array.isArray(w)) return SqlStringEscape(w);

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
    list.push('`' + i + '`=' + SqlStringEscape(w[i]));
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
  var line = [escapeName(name)];
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
      line.push('DEFAULT ' + (data.default === null ? 'NULL' : SqlStringEscape(data.default)));
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
  if (op === 'DROP') return 'DROP ' + escapeName(name);
  var sql = parseCreateTableField(name, data);
  if (op === 'CHANGE') return 'CHANGE ' + escapeName(name) + ' ' + sql;
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
    return 'KEY ' + escapeName(data) + '(' + escapeName(data) + ')';
  } else if (Array.isArray(data)) {
    var k = data.join('_');
    var f = data.map(escapeName).join(', ');
    return 'KEY ' + escapeName(k) + '(' + f + ')';
  } else {
    if (Array.isArray(data.fields)) {
      var k = data.fields.join('_');
      var f = data.fields.map(escapeName).join(', ');
    } else {
      var k = data.fields;
      var f = escapeName(data.fields);
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
function parseUpdateTableIndex (data) {
  if (typeof data === 'string' || Array.isArray(data)) {
    data = {fields: data};
  }

  data.primary = !!data.primary;
  data.unique = !!data.unique;
  data.fullText = !!data.fullText;
  return data;
}

/**
 * 生产修改表旧索引的数据结构
 *
 * @param {Array} list
 * @return {Array}
 */
function parseUpdateTableOldIndex (list) {
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

  return Object.keys(indexes).map(function (k) {
    var a = indexes[k];
    a.key = k;
    if (a.fields.length === 1) a.fields = a.fields[0];
    return a;
  });
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