'use strict'
let fs = require('fs')

let words = fs.readFileSync("jmdict/meanings.ger[].text", 'utf-8').split('\n')

console.log(words[1000])


for (var i = 0; i < words.length; i++) {
    if(words[i].indexOf("waschf") == 0){
        console.log(words[i])
        // return
    }
}



let buf = fs.readFileSync("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds")
let arr =  new Uint32Array(buf.buffer, buf.offset, buf.byteLength/4)

console.log(arr[100])





// console.log(source[800])
// fs.writeFileSync("words.txt", source.join('\n'),'utf-8')

let source2 = fs.readFileSync("words.txt", 'utf-8').split('\n')

// console.log(source2)
var levenshtein = require('fast-levenshtein');

console.time("abc")
for (var i = 1; i < source2.length; i++) {
    var distance = levenshtein.get(source2[i-1], source2[i]);   // 2
}
console.timeEnd("abc")