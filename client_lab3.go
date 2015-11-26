package main

import (
	"fmt"
	"sort"
	"net/http"
	"io/ioutil"
	"hash/crc32"
	"encoding/json"
)

type HashCircle []uint32

type KeyValue struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}


// implementing the consistent hashing functionality
func (hc HashCircle) Len() int {
	return len(hc)
}

func (hc HashCircle) Less(i, j int) bool {
	return hc[i] < hc[j]
}

func (hc HashCircle) Swap(i, j int) {
	hc[i], hc[j] = hc[j], hc[i]
}

type Node struct {
	Id int
	IP string
}

func NewNode(id int, ip string) *Node {
	return &Node{
		Id: id,
		IP: ip,
	}
}

type ConsistentHash struct {
	Nodes     map[uint32]Node
	IsPresent map[int]bool
	Circle    HashCircle
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		Nodes:     make(map[uint32]Node),
		IsPresent: make(map[int]bool),
		Circle:    HashCircle{},
	}
}

func (hc *ConsistentHash) AddNode(node *Node) bool {

	if _, ok := hc.IsPresent[node.Id]; ok {
		return false
	}
	str := hc.ReturnNodeIP(node)
	hc.Nodes[hc.GetHashValue(str)] = *(node)
	hc.IsPresent[node.Id] = true
	hc.SortHashCircle()
	return true
}

func (hc *ConsistentHash) SortHashCircle() {
	hc.Circle = HashCircle{}
	for k := range hc.Nodes {
		hc.Circle = append(hc.Circle, k)
	}
	sort.Sort(hc.Circle)
}

func (hc *ConsistentHash) ReturnNodeIP(node *Node) string {
	return node.IP
}

func (hc *ConsistentHash) GetHashValue(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (hc *ConsistentHash) Get(key string) Node {
	hash := hc.GetHashValue(key)
	i := hc.SearchForNode(hash)
	return hc.Nodes[hc.Circle[i]]
}

func (hc *ConsistentHash) SearchForNode(hash uint32) int {
	i := sort.Search(len(hc.Circle), func(i int) bool { return hc.Circle[i] >= hash })
	if i < len(hc.Circle) {
		if i == len(hc.Circle)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(hc.Circle) - 1
	}
}

//put the key-value into ring
func PutKeyVal(circle *ConsistentHash, str string, input string) {
	ipAddress := circle.Get(str)
	address := "http://" + ipAddress.IP + "/keys/" + str + "/" + input
	fmt.Printf("%s", address)
	req, err := http.NewRequest("PUT", address, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer resp.Body.Close()
		fmt.Printf("-------PUT successful\n")
	}
}

//get a particular key's value
func GetVal(key string, circle *ConsistentHash) {
	var out KeyValue
	ipAddress := circle.Get(key)
	address := "http://" + ipAddress.IP + "/keys/" + key
	fmt.Println(address)
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

//get all keys and values
func GetAll(address string) {
	var out []KeyValue
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

// hard-coded input keys-values
func main() {
	circle := NewConsistentHash()
	circle.AddNode(NewNode(0, "localhost:3000"))
	circle.AddNode(NewNode(1, "localhost:3001"))
	circle.AddNode(NewNode(2, "localhost:3002"))

  fmt.Println("\n____________________________________________________________\n")
	fmt.Println("Inserting data into cache:\n")
	PutKeyVal(circle, "1", "a")
	PutKeyVal(circle, "2", "b")
	PutKeyVal(circle, "3", "c")
	PutKeyVal(circle, "4", "d")
	PutKeyVal(circle, "5", "e")
	PutKeyVal(circle, "6", "f")
	PutKeyVal(circle, "7", "g")
	PutKeyVal(circle, "8", "h")
	PutKeyVal(circle, "9", "i")
	PutKeyVal(circle, "10", "j")

  fmt.Println("\n____________________________________________________________\n")

  fmt.Println("Retrieving single data from cache:\n")
	GetVal("1", circle)
	GetVal("2", circle)
	GetVal("3", circle)
	GetVal("4", circle)
	GetVal("5", circle)
	GetVal("6", circle)
	GetVal("7", circle)
	GetVal("8", circle)
	GetVal("9", circle)
	GetVal("10", circle)

  fmt.Println("\n____________________________________________________________\n")

  fmt.Println("Retrieving all data from cache:\n")
	GetAll("http://localhost:3000/keys")
	fmt.Println("\n\n")
	GetAll("http://localhost:3001/keys")
	fmt.Println("\n\n")
	GetAll("http://localhost:3002/keys")

}
