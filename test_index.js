'use strict'
let fs = require('fs')

fs.writeFileSync('index11', new Buffer(new Uint32Array([11]).buffer))


fs.writeFileSync('index11_12', new Buffer(new Uint32Array([11, 12, 13]).buffer))



console.log(load('index11')[0])


console.log(load('index11_12')[1])


function load(path) {
    let buf = fs.readFileSync(path)
    return new Uint32Array(buf.buffer, buf.offset, buf.byteLength/4)
}