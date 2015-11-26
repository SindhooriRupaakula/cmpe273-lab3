package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
  "net/http"
	"github.com/julienschmidt/httprouter"
)

type KeyValue struct {
	Key   int `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

var keyValues []KeyValue
var num int
type ByKey []KeyValue
func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func PutKeyValues(rw http.ResponseWriter, r *http.Request, p httprouter.Params) {
	key, _ := strconv.Atoi(p.ByName("key"))
	keyValues = append(keyValues, KeyValue{key, p.ByName("value")})
	num++
}

func GetKeyValues(rw http.ResponseWriter, r *http.Request, p httprouter.Params) {
		sort.Sort(ByKey(keyValues))
		result, _ := json.Marshal(keyValues)
		fmt.Fprintln(rw, string(result))
}

func GetValue(rw http.ResponseWriter, r *http.Request, p httprouter.Params) {
	key, _ := strconv.Atoi(p.ByName("key"))
	for i := 0; i < num; i++ {
		if keyValues[i].Key == key {
			result, _ := json.Marshal(keyValues[i])
			fmt.Fprintln(rw, string(result))
		}
	}
}

func main() {
	num = 0
	mux := httprouter.New()
  mux.PUT("/keys/:key/:value", PutKeyValues)
	mux.GET("/keys", GetKeyValues)
	mux.GET("/keys/:key", GetValue)
	go http.ListenAndServe(":3000", mux)
	select{}
}
