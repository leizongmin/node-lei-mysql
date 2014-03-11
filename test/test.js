/**
 * 测试
 */

var should = require('should');
var async = require('async');

var MySQLPool = require('../');
var db = new MySQLPool({
  host:     'localhost',
  port:     3306,
  database: 'test',
  user:     'root',
  password: '',
  pool:     '2'
});

var TABLE = 'test';


describe('Simple MySQL Pool', function () {

  var INIT_COUNT = 10;

  it('dropTable', function (done) {
    db.dropTable(TABLE, function (err, info) {
      should.equal(err, null);
      done();
    })
  });

  it('createTable', function (done) {
    db.createTable(TABLE, {
      id: {
        type: 'int',
        autoIncrement: true
      },
      value: {
        type: 'double',
        default: 0
      },
      timestamp: 'int'
    }, [
      {fields: 'id', primary: true}
    ], function (err, info) {
      should.equal(err, null);
      done();
    })
  });

  it('insert random data', function (done) {
    var lines = [];
    for (var i = 0; i < INIT_COUNT; i++) {
      lines.push({value: Math.random(), timestamp: db.timestamp() + i});
    }
    db.insert(TABLE, lines, done);
  });

  it('find all', function (done) {
    db.find(TABLE, true, function (err, list) {
      should.equal(err, null);
      should.equal(list.length, INIT_COUNT);
      done();
    });
  });

  it('find one', function (done) {
    db.findOne(TABLE, true, function (err, item) {
      should.equal(err, null);
      should.equal(item.timestamp > 0, true);
      done();
    });
  });

  it('insert one', function (done) {
    db.insert(TABLE, {value: 1, timestamp: 0}, function (err, info) {
      should.equal(err, null);
      should.equal(info.insertId > 0, true);
      should.equal(info.affectedRows, 1);
      done();
    });
  });

  it('insert multi', function (done) {
    db.insert(TABLE, [{value: 2, timestamp: 0}, {value: 3, timestamp: 0}], function (err, info) {
      should.equal(err, null);
      should.equal(info.insertId > 0, true);
      should.equal(info.affectedRows, 2);
      done();
    });
  });

  it('update - 1', function (done) {
    var where = '`value`=1';
    db.update(TABLE, where, {timestamp: 1}, function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 1);
      db.findOne(TABLE, where, function (err, item) {
        should.equal(err, null);
        should.equal(item.value, 1);
        should.equal(item.timestamp, 1);
        done();
      });
    });
  });

  it('update - 2', function (done) {
    var where = '`value`=1';
    db.update(TABLE, where, '`timestamp`=`timestamp`+10', function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 1);
      db.findOne(TABLE, where, function (err, item) {
        should.equal(err, null);
        should.equal(item.value, 1);
        should.equal(item.timestamp, 11);
        done();
      });
    });
  });

  it('update - 3', function (done) {
    var where = '`value`=1';
    db.update(TABLE, where, {timestamp: ['$incr', 2]}, function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 1);
      db.findOne(TABLE, where, function (err, item) {
        should.equal(err, null);
        should.equal(item.value, 1);
        should.equal(item.timestamp, 13);
        done();
      });
    });
  });

  it('update - 4', function (done) {
    var where = '`value`=1';
    db.update(TABLE, where, {timestamp: ['$incr', -5]}, function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 1);
      db.findOne(TABLE, where, function (err, item) {
        should.equal(err, null);
        should.equal(item.value, 1);
        should.equal(item.timestamp, 8);
        done();
      });
    });
  });

  it('delete', function (done) {
    var where = '`value`=1';
    db.delete(TABLE, where, function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 1);
      db.findOne(TABLE, where, function (err, item) {
        should.equal(err, null);
        should.equal(item, undefined);
        done();
      });
    });
  });

  it('where - 1', function (done) {
    db.find(TABLE, ['value', '>', 0.5], function (err, list) {
      should.equal(err, null);
      list.forEach(function (item) {
        should.equal(item.value > 0.5, true);
      });
      done();
    });
  });

  it('where - 2', function (done) {
    db.find(TABLE, ['$and', ['value', '>', 0.5], ['value', '<', 0.8]], function (err, list) {
      should.equal(err, null);
      list.forEach(function (item) {
        should.equal(item.value > 0.5 && item.value < 0.8, true);
      });
      done();
    });
  });

  it('where - 3', function (done) {
    db.find(TABLE, ['timestamp', 0], function (err, list) {
      should.equal(err, null);
      should.equal(list.length === 2, true);
      list.forEach(function (item) {
        should.equal(item.timestamp === 0, true);
      });
      done();
    });
  });

  it('where - 4', function (done) {
    db.find(TABLE, {timestamp: 0}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length === 2, true);
      list.forEach(function (item) {
        should.equal(item.timestamp === 0, true);
      });
      done();
    });
  });

  it('find - fields - 1', function (done) {
    db.find(TABLE, true, {fields: ['id', 'value']}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length > INIT_COUNT, true);
      list.forEach(function (item) {
        should.deepEqual(Object.keys(item), ['id', 'value']);
      });
      done();
    });
  });

  it('find - fields - 2', function (done) {
    db.find(TABLE, true, {fields: ['id']}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length > INIT_COUNT, true);
      list.forEach(function (item) {
        should.deepEqual(Object.keys(item), ['id']);
      });
      done();
    });
  });

  it('find - fields - 3', function (done) {
    db.find(TABLE, true, {fields: '`id`, `value`'}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length > INIT_COUNT, true);
      list.forEach(function (item) {
        should.deepEqual(Object.keys(item), ['id', 'value']);
      });
      done();
    });
  });

  it('find - tail', function (done) {
    db.find(TABLE, true, {tail: 'ORDER BY `value` DESC'}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length > INIT_COUNT, true);
      for (var i = 0; i < list.length - 1; i++) {
        var a = list[i];
        var b = list[i + 1];
        should.equal(a.value >= b.value, true);
      }
      done();
    });
  });

  it('count - 1', function (done) {
    db.count(TABLE, true, function (err, count) {
      should.equal(err, null);
      should.equal(count > INIT_COUNT, true);
      done();
    });
  });

  it('count - 2', function (done) {
    db.count(TABLE, {timestamp: 0}, function (err, count) {
      should.equal(err, null);
      should.equal(count === 2, true);
      done();
    });
  });

  // ===========================================================================

  it('createTable & showFields & showIndexes & updateTable', function (done) {
    var TABLE = 'test_' + Date.now();

    var INDEXES_1 = [
      'a',
      'b',
      {fields: ['a', 'b'], unique: true},
      {fields: 'c'},
      {fields: 'd', primary: true},
      {fields: 'e', fullText: true}
    ];
    var INDEXES_2 = [
      {fields: ['b', 'c']},
      {fields: 'c', primary: true},
      {fields: 'd'},
      {fields: 'e', fullText: true}
    ];

    async.series([
      // 创建表
      function (done) {
        db.createTable(TABLE, {
          a: 'int',
          b: {type: 'int', size: 10, default: 1},
          c: {type: 'varchar', size: 5, charset: 'utf8', null: true, default: 'a'},
          d: {type: 'int', autoIncrement: true},
          e: {type: 'text', charset: 'gbk'}
        }, INDEXES_1, done);
      },
      // 检查表结构
      function (done) {
        db.showFields(TABLE, function (err, fields) {
          if (err) return done(err);
          should.deepEqual(Object.keys(fields), ['a', 'b', 'c', 'd', 'e']);
          should.deepEqual(fields, {
            a: {
              type: 'INT',
              size: '11',
              null: true,
              default: null,
              autoIncrement: false,
              primary: false,
              unique: false,
              index: true
            },
            b: {
              type: 'INT',
              size: '10',
              null: false,
              default: '1',
              autoIncrement: false,
              primary: false,
              unique: false,
              index: true
            },
          c: {
            type: 'VARCHAR',
            size: '5',
            null: true,
            default: 'a',
            autoIncrement: false,
            primary: false,
            unique: false,
            index: true
          },
          d: {
            type: 'INT',
            size: '11',
            null: false,
            default: null,
            autoIncrement: true,
            primary: true,
            unique: false,
            index: false
          },
          e: {
            type: 'TEXT',
            size: '',
            null: false,
            default: null,
            autoIncrement: false,
            primary: false,
            unique: false,
            index: true
          }});
          done();
        });
      },
      // 检查索引结构
      function (done) {
        db.showIndexes(TABLE, function (err, indexes) {
          should.equal(err, null);
          should.equal(compareIndexes(INDEXES_1, indexes), true);
          done();
        });
      },
      // 更新表结构
      function (done) {
        db.updateTable(TABLE, {
          b: {type: 'double', default: 1},
          c: {type: 'varchar', size: 5, charset: 'utf8', null: true, default: 'a'},
          d: {type: 'int', autoIncrement: true},
          e: {type: 'text', charset: 'gbk'},
          f: 'int'
        }, INDEXES_2, function (err) {
          should.equal(err, null);
          done();
        });
      },
      // 检查表结构
      function (done) {
        db.showFields(TABLE, function (err, fields) {
          if (err) return done(err);
          should.deepEqual(Object.keys(fields), ['b', 'c', 'd', 'e', 'f']);
          should.deepEqual(fields, {
            b: {
              type: 'DOUBLE',
              size: '',
              null: false,
              default: '1',
              autoIncrement: false,
              primary: false,
              unique: false,
              index: true
            },
            c: {
              type: 'VARCHAR',
              size: '5',
              null: false,
              default: 'a',
              autoIncrement: false,
              primary: true,
              unique: false,
              index: false
            },
            d: {
              type: 'INT',
              size: '11',
              null: false,
              default: null,
              autoIncrement: true,
              primary: false,
              unique: false,
              index: true
            },
            e: {
              type: 'TEXT',
              size: '',
              null: false,
              default: null,
              autoIncrement: false,
              primary: false,
              unique: false,
              index: true
            },
            f: {
              type: 'INT',
              size: '11',
              null: true,
              default: null,
              autoIncrement: false,
              primary: false,
              unique: false,
              index: false
          }});
          done();
        });
      },
      // 检查索引
      function (done) {
        db.showIndexes(TABLE, function (err, indexes) {
          should.equal(err, null);
          should.equal(compareIndexes(INDEXES_2, indexes), true);
          done();
        });
      },
      // 删除表
      function (done) {
        db.dropTable(TABLE, done);
      }
    ], function (err) {
      should.equal(err, null);
      done();
    });
  });

  // ===========================================================================

  it('dropTable', function (done) {
    db.dropTable(TABLE, function (err, info) {
      should.equal(err, null);
      done();
    })
  });

  it('createTable', function (done) {
    db.createTable(TABLE, {
      id: {
        type: 'int',
        autoIncrement: true
      },
      value: {
        type: 'double',
        default: 0
      },
      timestamp: 'int'
    }, [
      {fields: 'id', primary: true}
    ], function (err, info) {
      should.equal(err, null);
      done();
    })
  });

  it('insert random data', function (done) {
    var lines = [];
    for (var i = 0; i < INIT_COUNT; i++) {
      lines.push({value: Math.random(), timestamp: db.timestamp() + i});
    }
    db.insert(TABLE, lines, done);
  });

  function initFilters () {
    db._filters = {
      sql:    [],
      result: [],
      error:  []
    };
  }

  it('use(result)', function (done) {
    initFilters();
    db.use('result', function (sql, list, next) {
      should.equal(list.length, INIT_COUNT);
      next(null, list.slice(0, 5));
    });
    db.use('result', function (sql, list, next) {
      should.equal(list.length, 5);
      next(null, list.slice(0, 3));
    });
    db.find(TABLE, true, function (err, list) {
      should.equal(err, null);
      should.equal(list.length, 3);
      done();
    });
  });
return;
  it('use(sql) - 1', function (done) {
    initFilters();
    var sql1 = 'SELECT * FROM `' + TABLE + '`';
    var sql2 = sql1 + ' LIMIT 4';
    db.use('sql', function (sql, next) {
      should.equal(sql, sql1);
      next(null, sql2);
    });
    db.use('select', function (sql, next) {
      should.equal(sql, sql2);
      next(null, sql);
    });
    db.use('update', function (sql, next) {
      next(new Error('at this time should not call the update function'));
    });
    db.query(sql1, function (err, list) {
      should.equal(err, null);
      should.equal(list.length, 4);
      done();
    });
  });

  it('use(sql) - 2', function (done) {
    initFilters();
    var sql1 = 'SELECT * FROM `' + TABLE + '`';
    var sql2 = sql1 + ' LIMIT 4';
    var data = [{a: 1}, {b: 2}];
    db.use('sql', function (sql, next) {
      should.equal(sql, sql1);
      next(null, sql2);
    });
    db.use('select', function (sql, next) {
      should.equal(sql, sql2);
      next(null, sql1);
    });
    db.use('sql', function (sql, next) {
      should.equal(sql, sql1);
      next(null, data);
    });
    db.query(sql1, function (err, list) {
      should.equal(err, null);
      should.equal(list.length, data.length);
      should.deepEqual(list, data);
      done();
    });
  });

});


// 对比索引结构
function compareIndexes (listA, listB) {
  listA = listA.map(formatIndexes);
  listB = listB.map(formatIndexes);
  if (listA.length !== listB.length) return false;
  var count = 0;
  listA.forEach(function (a) {
    for (var i = 0; i < listB.length; i++) {
      var b = listB[i];
      if (fieldsIsEqual(a.fields, b.fields) && a.primary === b.primary &&
          a.unique === b.unique && a.fullText === b.fullText) {
        count++;
        break;
      }
    }
  });
  return (count === listA.length);
}

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

function formatIndexes (data) {
  if (typeof data === 'string' || Array.isArray(data)) {
    data = {fields: data};
  }

  data.primary = !!data.primary;
  data.unique = !!data.unique;
  data.fullText = !!data.fullText;
  return data;
}