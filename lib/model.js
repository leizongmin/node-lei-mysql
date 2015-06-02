/**
 * Simple MySQL Model
 *
 * @author 老雷 <leizongmin@gmail.com>
 */


var MAX_LIMIT = 9999999999999;

function formatQuery (query) {
  if (typeof query === 'string') return query;
  if (Array.isArray(query)) {
    if (query.length < 1) return 1;
  } else {
    if (Object.keys(query).length < 1) return 1;
  }
  return query;
}

function createModel (connection, name) {
  var model = {
    name: name,
    connection: connection,
    escape: connection.escape,
    escapeId: connection.escapeId
  };

  /**
   * parseListOptions
   *
   * @param {Object} options
   *   - {String} fields
   *   - {String} sort
   *   - {Number} skip
   *   - {Number} limit
   * @return {Object}
   */
  model.parseListOptions = function (options) {
    var opts = {};
    opts.fields = options.fields || '*';
    opts.tail = '';
    if (options.sort) opts.tail += ' ORDER BY ' + (Array.isArray(options.sort) ? options.sort.join(', ') : options.sort);
    var skip = Number(options.skip);
    if (!(skip > 0)) skip = 0;
    var limit = Number(options.limit);
    if (!(limit > 0)) limit = MAX_LIMIT;
    opts.tail += ' LIMIT ' + skip + ',' + limit;
    return opts;
  };

  /**
   * find
   *
   * @param {Object} query
   * @param {Object} options
   * @param {Function} callback
   */
  model.find = function (query, options, callback) {
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    return connection.find(name, formatQuery(query), model.parseListOptions(options), callback);
  };

  /**
   * findOne
   *
   * @param {Object} query
   * @param {Function} callback
   */
  model.findOne = function (query, callback) {
    return connection.findOne(name, formatQuery(query), callback);
  };

  /**
   * create
   *
   * @param {Object} data
   * @param {Function} callback
   */
  model.create = function (data, callback) {
    return connection.insert(name, data, callback);
  };

  /**
   * delete
   *
   * @param {Object} query
   * @param {Function} callback
   */
  model.delete = function (query, callback) {
    return connection.delete(name, formatQuery(query), 'LIMIT 1', callback);
  };

  /**
   * deleteAll
   *
   * @param {Object} query
   * @param {Function} callback
   */
  model.deleteAll = function (query, callback) {
    return connection.delete(name, formatQuery(query), callback);
  };

  /**
   * update
   *
   * @param {Object} query
   * @param {Object|array} update
   * @param {Function} callback
   */
  model.update = function (query, update, callback) {
    return connection.update(name, formatQuery(query), update, 'LIMIT 1',  callback);
  };

  /**
   * updateAll
   *
   * @param {Object} query
   * @param {Object|array} update
   * @param {Function} callback
   */
  model.updateAll = function (query, update, callback) {
    return connection.update(name, formatQuery(query), update, callback);
  };

  /**
   * count
   *
   * @param {Object} query
   * @param {Function} callback
   */
  model.count = function (query, callback) {
    return connection.count(name, formatQuery(query), callback);
  };

  /**
   * incr
   *
   * @param {Object} query
   * @param {Object} update
   * @param {Function} callback
   */
  model.incr = function (query, update, callback) {
    for (var i in update) {
      update[i] = ['$incr', update[i]];
    }
    connection.update(name, formatQuery(query), update, 'LIMIT 1', callback);
  };

  return model;
}

module.exports = exports = createModel;
