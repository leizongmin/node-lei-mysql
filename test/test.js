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

  var INIT_COUNT = 1000;

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

  // ===========================================================================

  it('createTable & showFields & showIndexes', function (done) {
    var TABLE = 'test_' + Date.now();

    async.series([
      function (done) {
        db.createTable(TABLE, {
          a: 'int',
          b: {type: 'int', size: 10, default: 1},
          c: {type: 'varchar', size: 5, charset: 'utf8', null: true, default: 'a'},
          d: {type: 'int', autoIncrement: true},
          e: {type: 'text', charset: 'gbk'}
        }, [
          'a',
          'b',
          {fields: ['a', 'b'], unique: true},
          {fields: 'c'},
          {fields: 'd', primary: true},
          {fields: 'e', fullText: true}
        ], done);
      },
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
      }, function (done) {
        db.updateTable(TABLE, {
          b: {type: 'double', default: 1},
          c: {type: 'varchar', size: 5, charset: 'utf8', null: true, default: 'a'},
          d: {type: 'int', autoIncrement: true},
          e: {type: 'text', charset: 'gbk'},
          f: 'int'
        }, [
          'a',
          'b',
          {fields: ['a', 'b']},
          //{fields: 'c'},
          {fields: 'd', primary: true},
          {fields: 'e', fullText: true}
        ], function (err) {
          should.equal(err, null);
          done();
        });
      }, function (done) {
        db.dropTable(TABLE, done);
      }
    ], function (err) {
      should.equal(err, null);
      done();
    });
  });

});