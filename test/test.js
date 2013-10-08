/**
 * 测试
 */

var should = require('should');

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

function wait (callback) {
  if (wait._done) return callback(null);
  // 创建初始化数据表
  var sql = 'CREATE TABLE IF NOT EXISTS `test` (\n' +
            '  `id` INT NOT NULL AUTO_INCREMENT ,\n' +
            '  `value` DOUBLE NOT NULL ,\n' +
            '  `timestamp` INT NOT NULL ,\n' +
            '  PRIMARY KEY (`id`)' +
            '  ) ENGINE=MyISAM AUTO_INCREMENT=1';
  db.query(sql, function (err) {
    wait._done = true;
    callback(err);
  });
}

describe('Simple MySQL Pool', function () {

  var INIT_COUNT = 1000;

  it('remove all lines', function (done) {
    wait(function () {
      db.delete(TABLE, '1', done);
    });
  });

  it('showFields', function (done) {
    db.showFields('test2', function (err, fields) {
      should.equal(err, null);
      done();
    });
  });

  it('showIndexes', function (done) {
    db.showIndexes('test2', function (err, indexes) {
      should.equal(err, null);
      done();
    });
  });

  return;
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

  it('update', function (done) {
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

});