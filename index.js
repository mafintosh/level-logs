var lexint = require('lexicographic-integer')
var collect = require('stream-collector')
var through = require('through2')
var from = require('from2')
var pump = require('pump')

var noop = function () {}

var Logs = function (db, opts) {
  if (!(this instanceof Logs)) return new Logs(db, opts)
  if (!opts) opts = {}

  this.db = db
  this.sep = opts.separator || opts.sep || '!'
  this.prefix = opts.prefix ? this.sep + opts.prefix + this.sep : ''
  this.valueEncoding = opts.valueEncoding

  // Global lock used to ensure sequential numbering.
  this.locked = false
  // FIFO of write operations waiting for the global lock.
  // [[method, args...], [method, args...], ...]
  this.pending = []
}

Logs.prototype.key = function (log, seq) {
  return this.prefix + log + this.sep + (seq === -1 ? '\xff' : lexint.pack(seq, 'hex'))
}

Logs.prototype.list = function (cb) {
  var self = this

  var prev = this.prefix
  var rs = from.obj(function (size, cb) {
    collect(self.db.createKeyStream({gt: prev, limit: 1}), function (err, keys) {
      if (err) return cb(err)
      if (!keys.length) return cb(null, null)
      var log = keys[0].slice(0, keys[0].lastIndexOf(self.sep))
      prev = self.key(log, -1)
      cb(null, log)
    })
  })

  return collect(rs, cb)
}

Logs.prototype.get = function (log, seq, cb) {
  this.db.get(this.key(log, seq), {valueEncoding: this.valueEncoding}, cb)
}

Logs.prototype.put = function put (log, seq, value, cb) {
  var self = this

  // If the current head of log "x" is 55 and we put("x", 99, value),
  // the put will advance the head of "x" to 99. Since puts can advance
  // the head of a log, we need the global lock for puts, too.
  if (self.locked) return this.pending.push([put, log, seq, value, cb])
  self.locked = true
  var opts = {valueEncoding: self.valueEncoding}
  self.db.put(self.key(log, seq), value, opts, function (err) {
    doPending.call(self)
    cb(err)
  })
}

Logs.prototype.append = function append (log, value, cb) {
  var self = this

  if (!cb) cb = noop
  // Because head() is async, sync calls like
  //
  //     logs.append("x", "a")
  //     logs.append("x", "b")
  //
  // may calculate the same head value and try to write to the same key.
  // As a result, the second append call will overwrite "a" with "b" at
  // new head.
  //
  // To prevent this, every write to the underlying LevelUP takes out
  // a global lock. While locked, all other put and append calls get
  // queued for execution when the lock becomes available again.
  //
  // The problem does't affect nested async appends like:
  //
  //     logs.append("x", "a", function () {
  //         logs.append("x", "b", function () {
  //             ...
  //         })
  //     })
  //
  if (self.locked) return this.pending.push([append, log, value, cb])
  self.locked = true
  self.head(log, function (err, head) {
    if (err) {
      doPending.call(self)
      return cb(err)
    }
    var seq = head + 1
    var key = self.key(log, seq)
    var opts = {valueEncoding: self.valueEncoding}
    self.db.put(key, value, opts, function (err) {
      doPending.call(self)
      cb(err, seq)
    })
  })
}

// Unlock and execute the next waiting write operation.
function doPending () {
  this.locked = false
  if (this.pending.length !== 0) {
    var next = this.pending.shift()
    next[0].apply(this, next.slice(1))
  }
}

Logs.prototype.head = function (log, cb) {
  var self = this

  var keys = this.db.createKeyStream({
    gt: this.key(log, 0),
    lt: this.key(log, -1),
    reverse: true,
    limit: 1
  })

  collect(keys, function (err, head) {
    if (err) return cb(err)
    cb(null, head.length ? lexint.unpack(head[0].slice(head[0].lastIndexOf(self.sep) + 1), 'hex') : 0)
  })
}

Logs.prototype.createValueStream = function (log, opts) {
  if (!opts) opts = {}
  return this.db.createValueStream({
    gt: this.key(log, opts.since || 0),
    lt: this.key(log, opts.until || -1),
    valueEncoding: this.valueEncoding,
    reverse: opts.reverse
  })
}

Logs.prototype.createReadStream = function (log, opts) {
  if (!opts) opts = {}

  var self = this

  var rs = this.db.createReadStream({
    gt: this.key(log, opts.since || 0),
    lt: this.key(log, opts.until || -1),
    valueEncoding: this.valueEncoding,
    reverse: opts.reverse
  })

  var format = through.obj(function (data, enc, cb) {
    var key = data.key
    var log = key.slice(0, key.lastIndexOf(self.sep))
    var seq = lexint.unpack(key.slice(log.length + 1), 'hex')
    cb(null, {log: log, seq: seq, value: data.value})
  })

  return pump(rs, format)
}

module.exports = Logs
