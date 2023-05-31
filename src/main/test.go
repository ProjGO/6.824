package main

import (
	"6.5840/shardctrler"
)

func copyMap(m map[int]int) map[int]int {
	nm := make(map[int]int)

	for k, v := range m {
		nm[k] = v
	}

	return nm
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func main() {
	// tempFilepath := "./mr-tmp"
	// file, _ := ioutil.TempFile(tempFilepath, "mr-tmp-*")
	// fmt.Println(file.Name())

	// kv := mr.KeyValue{"1", "2"}
	// enc := json.NewEncoder(file)
	// enc.Encode(&kv)

	// os.Rename(file.Name(), filepath.Join(tempFilepath, "test.json"))
	// file.Close()

	// time1 := time.Now()

	// time.Sleep(3 * time.Second)

	// fmt.Println(time.Since(time1))

	// expired := time.Since(time1) > time.Duration(2*time.Second)
	// fmt.Println(expired)

	// for i := 0; i < 3; i++ {
	// 	go func(x int) {
	// 		time.Sleep(3 * time.Second)

	// 		fmt.Println(x)
	// 	}(i)
	// }

	// fmt.Println("end")

	// map is passed by "value of address" (== reference)
	// m1 := make(map[int]int)

	// m1[1] = 1

	// println(m1[1])

	// m2 := m1
	// m2[1] = 2

	// println(m1[1])

	// m3 := copyMap(m1)
	// m3[1] = 3

	// println(m1[1], m3[1])

	// strArr1 := make([]string, 10)
	// strArr1[0] = "1234"
	// println(strArr1[0])

	// strArr2 := strArr1
	// strArr2[0] = "2345"
	// println(strArr1[0])

	// intArr1 := make([]int, 10)
	// intArr1[0] = 1
	// println(intArr1[0])

	// intArr2 := intArr1
	// intArr2[0] = 2
	// println(intArr1[0])

	// fmt.Printf("%+v\n", m1)
	// fmt.Printf("%+v\n", intArr1)

	m4 := make(map[int]int)
	m4[1] = 1
	m4[2] = 2
	m4[3] = 3
	for k, v := range m4 {
		println(k, v)
	}
	for k := range m4 {
		println(k)
	}
	// v, ok := m4[2]
	// print(v, ok)
	// v, ok = m4[4]
	// print(v, ok)

	// to get the shard of key
	key := "2"
	shard := key2shard(key)
	println(shard)

	sli := make([]int, 1)
	println((sli[0])) // ok
	// println((sli[1]))  // error

	// var ch1 chan string = make(chan string)
	// close(ch1)
	// func() {
	// 	time.Sleep(1 * time.Second)
	// 	var str string = "123"
	// 	fmt.Printf("sending %v to ch1\n", str)
	// 	ch1 <- str
	// 	fmt.Printf("go func is done\n")
	// }()
	// // fmt.Printf("string is %v\n", <-ch1)
	// close(ch1)
	// fmt.Printf("main is done\n")
}
