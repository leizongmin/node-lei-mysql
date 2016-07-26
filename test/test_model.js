/**
 * 测试model
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

var TABLE = 'test2';


describe('Simple MySQL Model', function () {

  var INIT_COUNT = 10;

  it('dropTable', function (done) {
    db.dropTable(TABLE, function (err, info) {
      should.equal(err, null);
      done();
    });
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
    });
  });

  it('insert random data', function (done) {
    var lines = [];
    for (var i = 0; i < INIT_COUNT; i++) {
      lines.push({value: Math.random(), timestamp: db.timestamp() + i});
    }
    db.model(TABLE).create(lines, done);
  });

  it('find all', function (done) {
    db.model(TABLE).find({}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length, INIT_COUNT);
      done();
    });
  });

  it('find one', function (done) {
    db.model(TABLE).findOne({}, function (err, item) {
      should.equal(err, null);
      should.equal(item.timestamp > 0, true);
      done();
    });
  });

  it('insert one', function (done) {
    db.model(TABLE).create({value: 1, timestamp: 0}, function (err, info) {
      should.equal(err, null);
      should.equal(info.insertId > 0, true);
      should.equal(info.affectedRows, 1);
      done();
    });
  });

  it('insert multi', function (done) {
    db.model(TABLE).create([{value: 2, timestamp: 0}, {value: 3, timestamp: 0}], function (err, info) {
      should.equal(err, null);
      should.equal(info.insertId > 0, true);
      should.equal(info.affectedRows, 2);
      done();
    });
  });

  it('update - 1', function (done) {
    var where = '`value`=1';
    db.model(TABLE).updateAll(where, {timestamp: 1}, function (err, info) {
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
    db.model(TABLE).updateAll(where, '`timestamp`=`timestamp`+10', function (err, info) {
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

  it('update - 3 如果要更新的data为空对象，不执行而直接返回', function (done) {
    var where = '`value`=1';
    db.model(TABLE).updateAll(where, '', function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 0);
      db.findOne(TABLE, where, function (err, item) {
        should.equal(err, null);
        should.equal(item.value, 1);
        should.equal(item.timestamp, 11);
        done();
      });
    });
  });

  it('incr - 1', function (done) {
    var where = '`value`=1';
    db.model(TABLE).incr(where, {timestamp: 2}, function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 1);
      db.model(TABLE).findOne(where, function (err, item) {
        should.equal(err, null);
        should.equal(item.value, 1);
        should.equal(item.timestamp, 13);
        done();
      });
    });
  });

  it('incr - 2', function (done) {
    var where = '`value`=1';
    db.model(TABLE).updateAll(where, {timestamp: ['$incr', -5]}, function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 1);
      db.model(TABLE).findOne(where, function (err, item) {
        should.equal(err, null);
        should.equal(item.value, 1);
        should.equal(item.timestamp, 8);
        done();
      });
    });
  });

  it('delete', function (done) {
    var where = '`value`=1';
    db.model(TABLE).deleteAll(where, function (err, info) {
      should.equal(err, null);
      should.equal(info.affectedRows, 1);
      db.model(TABLE).findOne(where, function (err, item) {
        should.equal(err, null);
        should.equal(item, undefined);
        done();
      });
    });
  });

  it('where - 1', function (done) {
    db.model(TABLE).find(['value', '>', 0.5], function (err, list) {
      should.equal(err, null);
      list.forEach(function (item) {
        should.equal(item.value > 0.5, true);
      });
      done();
    });
  });

  it('where - 2', function (done) {
    db.model(TABLE).find(['$and', ['value', '>', 0.5], ['value', '<', 0.8]], function (err, list) {
      should.equal(err, null);
      list.forEach(function (item) {
        should.equal(item.value > 0.5 && item.value < 0.8, true);
      });
      done();
    });
  });

  it('where - 3', function (done) {
    db.model(TABLE).find(['timestamp', 0], function (err, list) {
      should.equal(err, null);
      should.equal(list.length === 2, true);
      list.forEach(function (item) {
        should.equal(item.timestamp === 0, true);
      });
      done();
    });
  });

  it('where - 4', function (done) {
    db.model(TABLE).find({timestamp: 0}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length === 2, true);
      list.forEach(function (item) {
        should.equal(item.timestamp === 0, true);
      });
      done();
    });
  });

  it('find - fields - 1', function (done) {
    db.model(TABLE).find({}, {fields: ['id', 'value']}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length > INIT_COUNT, true);
      list.forEach(function (item) {
        should.deepEqual(Object.keys(item), ['id', 'value']);
      });
      done();
    });
  });

  it('find - fields - 2', function (done) {
    db.model(TABLE).find({}, {fields: ['id']}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length > INIT_COUNT, true);
      list.forEach(function (item) {
        should.deepEqual(Object.keys(item), ['id']);
      });
      done();
    });
  });

  it('find - fields - 3', function (done) {
    db.model(TABLE).find({}, {fields: '`id`, `value`'}, function (err, list) {
      should.equal(err, null);
      should.equal(list.length > INIT_COUNT, true);
      list.forEach(function (item) {
        should.deepEqual(Object.keys(item), ['id', 'value']);
      });
      done();
    });
  });

  it('find - sort - 1', function (done) {
    db.model(TABLE).find({}, {sort: '`value` DESC'}, function (err, list) {
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

  it('find - sort - 2', function (done) {
    db.model(TABLE).find({}, {sort: ['`value` DESC']}, function (err, list) {
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
    db.model(TABLE).count({}, function (err, count) {
      should.equal(err, null);
      should.equal(count > INIT_COUNT, true);
      done();
    });
  });

  it('count - 2', function (done) {
    db.model(TABLE).count({timestamp: 0}, function (err, count) {
      should.equal(err, null);
      should.equal(count === 2, true);
      done();
    });
  });

});
