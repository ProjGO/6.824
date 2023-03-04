package main

import (
	"fmt"
	"time"
)

func main() {
	// tempFilepath := "./mr-tmp"
	// file, _ := ioutil.TempFile(tempFilepath, "mr-tmp-*")
	// fmt.Println(file.Name())

	// kv := mr.KeyValue{"1", "2"}
	// enc := json.NewEncoder(file)
	// enc.Encode(&kv)

	// os.Rename(file.Name(), filepath.Join(tempFilepath, "test.json"))
	// file.Close()

	time1 := time.Now()

	time.Sleep(3 * time.Second)

	fmt.Println(time.Since(time1))

	expired := time.Since(time1) > time.Duration(2*time.Second)
	fmt.Println(expired)
}
