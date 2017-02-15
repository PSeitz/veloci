'use strict'
let fs = require('fs')

class IndexKeyValueStore{
    constructor(key, value1, value2){
        this.keys = typeof key === 'string' ? getIndex(key) : key
        this.values = typeof value1 === 'string' ? getIndex(value1) : value1
        if(value2) this.values2 = typeof value2 === 'string' ? getIndex(value2) : value2
    }
    getValue(key){
        let pos = binarySearch(this.keys, key)
        return this.values[pos]
    }
    getValues(key){
        let rows = binarySearchAll(this.keys, key)
        return rows.map(row => this.values[row])
    }
    getValue2(key){
        let pos = binarySearch(this.keys, key)
        return this.values2[pos]
    }
}

function getIndex(path){
    let buf = fs.readFileSync(path)
    return new Uint32Array(buf.buffer, buf.offset, buf.byteLength/4)
}

let kvStore = new IndexKeyValueStore("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds", "jmdict/meanings.ger[].text.textindex.valueIdToParent.mainIds")    

console.log(kvStore.values[100])

console.log(kvStore.getValue(100))
return
let words = fs.readFileSync("jmdict/meanings.ger[].text", 'utf-8').split('\n')

console.log(words[1000])


for (var i = 0; i < words.length; i++) {
    if(words[i].indexOf("waschf") == 0){
        console.log(words[i])
        // return
    }
}



let buf = fs.readFileSync("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds")



console.log(buf[400])

let arr =  new Uint32Array(buf.buffer, buf.offset, buf.byteLength/4)

console.log(arr[100])





// console.log(source[800])
// fs.writeFileSync("words.txt", source.join('\n'),'utf-8')



// console.log(source2)
var levenshtein = require('fast-levenshtein');

console.time("abc")
let source2 = fs.readFileSync("words.txt", 'utf-8').split('\n')
for (var i = 1; i < source2.length; i++) {
    var distance = levenshtein.get("test123", source2[i]);   // 2
}
console.timeEnd("abc")







function binarySearch(arr, find) {
    let low = 0, high = arr.length - 1,i
    while (low <= high) {
        i = Math.floor((low + high) / 2)
    // comparison = comparator(arr[i], find);
        if (arr[i] < find) { low = i + 1; continue }
        if (arr[i] > find) { high = i - 1; continue }
        return i
    }
    return null
}