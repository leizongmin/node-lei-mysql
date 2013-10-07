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

  it('insert random data', function (done) {
    var lines = [];
    for (var i = 0; i < INIT_COUNT; i++) {
      lines.push({value: Math.random(), timestamp: db.timestamp() + i});
    }
    db.insert(TABLE, lines, done);
  });

  it('select all', function (done) {
    db.select(TABLE, '*', '1', function (err, list) {
      should.equal(err, null);
      should.equal(list.length, INIT_COUNT);
      done();
    });
  });

  it('select one', function (done) {
    db.selectOne(TABLE, '*', '1', function (err, item) {
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
      db.selectOne(TABLE, '*', where, function (err, item) {
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
      db.selectOne(TABLE, '*', where, function (err, item) {
        should.equal(err, null);
        should.equal(item, undefined);
        done();
      });
    });
  });

  it('where - 1', function (done) {
    db.select(TABLE, '*', ['value', '>', 0.5], function (err, list) {
      should.equal(err, null);
      list.forEach(function (item) {
        should.equal(item.value > 0.5, true);
      });
      done();
    });
  });

  it('where - 2', function (done) {
    db.select(TABLE, '*', ['$and', ['value', '>', 0.5], ['value', '<', 0.8]], function (err, list) {
      should.equal(err, null);
      list.forEach(function (item) {
        should.equal(item.value > 0.5 && item.value < 0.8, true);
      });
      done();
    });
  });

  it('where - 3', function (done) {
    db.select(TABLE, '*', ['timestamp', 0], function (err, list) {
      should.equal(err, null);
      should.equal(list.length === 2, true);
      list.forEach(function (item) {
        should.equal(item.timestamp === 0, true);
      });
      done();
    });
  });

  it('where - 4', function (done) {
    db.select(TABLE, '*', {timestamp: 0}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length === 2, true);
      list.forEach(function (item) {
        should.equal(item.timestamp === 0, true);
      });
      done();
    });
  });

});