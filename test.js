var tape = require('tape')
var collect = require('stream-collector')
var memdb = require('memdb')
var logs = require('./')

tape('can add', function (t) {
  var lgs = logs(memdb(), {valueEncoding: 'json'})

  lgs.append('mathias', {hello: 'world'}, function () {
    lgs.append('mathias', {hej: 'verden'}, function () {
      collect(lgs.createReadStream('mathias'), function (err, datas) {
        if (err) throw err

        t.same(datas, [{
          log: 'mathias',
          seq: 1,
          value: {
            hello: 'world'
          }
        }, {
          log: 'mathias',
          seq: 2,
          value: {
            hej: 'verden'
          }
        }], 'saved logs')
        t.end()
      })
    })
  })
})

tape('append sequence numbers', function (t) {
  var lgs = logs(memdb(), {valueEncoding: 'json'})
  var returned = 0

  lgs.append('mathias', {hello: 'world'}, function (err, seq) {
    t.ifError(err, 'no error')
    t.equal(seq, 1, 'first gets seq 1')
    returned++
    done()
  })
  lgs.append('mathias', {hej: 'verden'}, function (err, seq) {
    t.ifError(err, 'no error')
    t.equal(seq, 2, 'second gets seq 2')
    returned++
    done()
  })
  function done () {
    if (returned === 2) t.end()
  }
})
