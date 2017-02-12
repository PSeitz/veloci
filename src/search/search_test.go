package main

import (
    "fmt"
    "io/ioutil"
    "strings"
    // "encoding/binary"
    "reflect"
    "unsafe"
    "sort"
    "github.com/texttheater/golang-levenshtein/levenshtein"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {

    b, err := ioutil.ReadFile("jmdict/meanings.ger[].text") // just pass the file name
    if err != nil {
        fmt.Print(err)
    }

    //fmt.Println(b) // print the content as 'bytes'

    str := string(b) // convert content to a 'string'
    stringSlice := strings.Split(str, "\n")

    fmt.Println(stringSlice[1000]) // print the content as a 'string'
    fmt.Printf("hello, worldoo\n")

    for _,element := range stringSlice {
        if strings.HasPrefix(element, "waschf") {
            fmt.Printf(element)
            // return
        }
    }

    // var a [10]int

    // raw, err := ioutil.ReadFile("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds") // just pass the file name
    // if err != nil {
    //     fmt.Print(err)
    // }

    // const SIZEOF_INT32 = 4 // bytes

    // // Get the slice header
    // header := *(*reflect.SliceHeader)(unsafe.Pointer(&raw))
    // // The length and capacity of the slice are different.
    // header.Len /= SIZEOF_INT32
    // header.Cap /= SIZEOF_INT32

    // Convert slice header to an []int32
    // data := loadIndex("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds")

    // fmt.Println(valIds)
    r1 := IndexKeyValueStore{loadIndex("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds"), loadIndex("jmdict/meanings.ger[].text.textindex.valueIdToParent.mainIds")}
    // var mySlice = []byte{244, 244, 244, 244, 244, 244, 244, 244}
    // data := binary.BigEndian.Uint32(valIds)
    fmt.Println(r1.values1[100])

    source := "a"
    target := "aa"
    distance := levenshtein.DistanceForStrings([]rune(source), []rune(target), levenshtein.DefaultOptions)
    fmt.Printf(`Distance between "%s" and "%s" computed as %d`, source, target, distance)

    words, err := ioutil.ReadFile("words.txt") 
    wordsstr := string(b) // convert content to a 'string'
    wordsArr := strings.Split(wordsstr, "\n")

    for i := 1; i <  len(wordsArr) i++ {
        distance := levenshtein.DistanceForStrings(rune(wordsArr[i-1]), []rune(wordsArr[i]), levenshtein.DefaultOptions)
        fmt.Printf(`Distance between "%s" and "%s" computed as %d`, source, target, distance)
    }
}

func loadIndex(path string) []int32 {
    raw, err := ioutil.ReadFile(path) // just pass the file name
    if err != nil {
        fmt.Print(err)
    }

    const SIZEOF_INT32 = 4 // bytes

    // Get the slice header
    header := *(*reflect.SliceHeader)(unsafe.Pointer(&raw))
    // The length and capacity of the slice are different.
    header.Len /= SIZEOF_INT32
    header.Cap /= SIZEOF_INT32

    // Convert slice header to an []int32
    data := *(*[]int32)(unsafe.Pointer(&header))
    return data
}

type IndexKeyValueStore struct {
    values1, values2 []int32 
}

func (kv IndexKeyValueStore) getValue(val int32) (int32, bool) {

    pos := sort.Search(len(kv.values1), func(i int) bool { return kv.values1[i] >= val })
    if pos < len(kv.values1) && kv.values1[pos] == val {
        // val is present at kv.values1[pos]
        return kv.values2[pos], true
    } else {
        // val is not present in kv.values1,
        // but pos is the index where it would be inserted.
        return -1, false
    }

    // pos = sort.SearchInts(kv.values1, val)

}

func (kv IndexKeyValueStore) getValues() {

}

    // getValue(key){
        
    //     let pos = binarySearch(this.keys, key)
    //     return this.values[pos]
    // }
    // getValues(key){
    //     let rows = binarySearchAll(this.keys, key)
    //     return rows.map(row => this.values[row])
    // }

// class IndexKeyValueStore{
//     constructor(key, value1, value2){
//         this.keys = typeof key === 'string' ? getIndex(key) : key
//         this.values = typeof value1 === 'string' ? getIndex(value1) : value1
//         if(value2) this.values2 = typeof value2 === 'string' ? getIndex(value2) : value2
//     }
//     getValue(key){
//         let pos = binarySearch(this.keys, key)
//         return this.values[pos]
//     }
//     getValues(key){
//         let rows = binarySearchAll(this.keys, key)
//         return rows.map(row => this.values[row])
//     }
//     getValue2(key){
//         let pos = binarySearch(this.keys, key)
//         return this.values2[pos]
//     }
// }