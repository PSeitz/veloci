package main

import (
    "fmt"
    "io/ioutil"
    "strings"
    // "encoding/binary"
    "reflect"
    "unsafe"
    "sort"
    // "github.com/texttheater/golang-levenshtein/levenshtein"
    "time"
    // "github.com/arbovm/levenshtein"
    // "unicode/utf8"
    // "bytes"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func newRuneIterator(completeString  []rune, sep rune) func() ([]rune, bool) {
    currentPos := 0
    length := len(completeString)
    // closure captures variable currentPos
    return func() ([]rune, bool) {

        var inc = 1
        for (completeString)[currentPos+inc] != sep{
            inc ++
            if length == currentPos+inc {
                return nil, false
            }
        }

        slice := (completeString)[currentPos:currentPos+inc]
        return slice, true
    }
}

func main() {


    // fmt.Printf(`Distance between "%s" and "%s" computed as %d`, "jaa", "m", Distance([]rune("jaa"), []rune("m")))

    // return

    b, err := ioutil.ReadFile("jmdict/meanings.ger[].text") // just pass the file name
    if err != nil {
        fmt.Print(err)
    }

    start2 := time.Now()
    str := string(b) // convert content to a 'string'
    
    stringSlice := strings.Split(str, "\n")

    meaningsger := make([][]rune, len(stringSlice))
    for i := 0; i < len(stringSlice); i++ {
        meaningsger[i] = []rune(stringSlice[i])
    }

    fmt.Printf("\ntime: convert to string array and then rune arry  %s \n", time.Since(start2))

    fmt.Println(stringSlice[1000]) // print the content as a 'string'
    for _,element := range stringSlice {
        if strings.HasPrefix(element, "waschf") {
            fmt.Printf(element)
            // return
        }
    }
    start3 := time.Now()
    fmt.Printf("\nconvert to string to rune iterator time %s", time.Since(start3))

    // fmt.Println(valIds)
    r1 := IndexKeyValueStore{loadIndex("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds"), loadIndex("jmdict/meanings.ger[].text.textindex.valueIdToParent.mainIds")}
    fmt.Println(r1.values1[100])

    source := "a"
    target := "aa"

    // distance := levenshtein.DistanceForStrings([]rune(source), []rune(target), levenshtein.DefaultOptions)
    fmt.Printf(`Distance between "%s" and "%s" computed as %d`, source, target, Distance([]rune(source), []rune(target)))

    fmt.Printf(`Distance between jaa and jaar computed as %d`, levenstheino([]rune("jaa"), []rune("jaar")))
    
    start := time.Now()

    words, err := ioutil.ReadFile("words.txt") 
    wordsstr := string(words) // convert content to a 'string'
    wordsArr := strings.Split(wordsstr, "\n")

    for i := 1; i < len(wordsArr); i++ {
        // levenshtein.DistanceForStrings([]rune(wordsArr[i-1]), []rune(wordsArr[i]), levenshtein.DefaultOptions)
        //fmt.Printf(`Distance between "%s" and "%s" computed as %d`, source, target, distance)
        Distance([]rune("test123"), []rune(wordsArr[i]))
    }

    fmt.Printf("\nBinomial took %s", time.Since(start))
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


var prevRow [500]int

func levenstheino(str1 []rune, str2 []rune) int {

    str1Len := len(str1)
    str2Len := len(str2)

    // prevRow := make([]int, str2Len+1)
    // str2Char := make([]rune, str2Len+1)

    // base cases
    if str1Len == 0 {return str2Len}
    if str2Len == 0 {return str1Len}

    // // two rows
    var curCol, nextCol, i, j, tmp int

    // initialise previous row
    for i=0; i<str2Len; i++ {
        prevRow[i] = i;
    }
    prevRow[str2Len] = str2Len;

    for i = 0; i < str1Len; i++ {
        nextCol = i + 1;

        for j = 0; j < str2Len; j++ {
            curCol = nextCol;

            nextCol = prevRow[j] // + (strCmp ? 0 : 1);
            if str1[i] != str2[j] { nextCol++ }
            // insertion
            tmp = curCol + 1;
            if (nextCol > tmp) {
                nextCol = tmp;
            }
            // deletion
            tmp = prevRow[j + 1] + 1;
            if (nextCol > tmp) {
                nextCol = tmp;
            }

            // copy current col value into previous (in preparation for next iteration)
            prevRow[j] = curCol;
        }

        // copy last col value into previous (in preparation for next iteration)
        prevRow[j] = nextCol;
    }

    return nextCol;
}


func Distance(s1 []rune, s2 []rune) int {
    var cost, lastdiag, olddiag int

    len_s1 := len(s1)
    len_s2 := len(s2)

    column := make([]int, len_s1+1)

    for y := 1; y <= len_s1; y++ {
        column[y] = y
    }

    for x := 1; x <= len_s2; x++ {
        // fmt.Printf("x: %d len_s2: %d\n", column[0], len_s2)
        column[0] = x
        // fmt.Printf("column[0]: %d\n", column[0])
        lastdiag = x - 1
        // fmt.Printf("lastdiag: %d\n", lastdiag)
        for y := 1; y <= len_s1; y++ {
            olddiag = column[y]
            cost = 0
            if s1[y-1] != s2[x-1] {
                cost = 1
            }
            column[y] = min(
                column[y]+1,
                column[y-1]+1,
                lastdiag+cost)
            // fmt.Printf("column[y]: %d\n", column[y])
            lastdiag = olddiag
        }
    }
    return column[len_s1]
}

func min(a, b, c int) int {
    if a < b {
        if a < c {
            return a
        }
    } else {
        if b < c {
            return b
        }
    }
    return c
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