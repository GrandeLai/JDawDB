package main

import (
	"github.com/GrandeLai/JDawDB"
)

func main() {
	opts := JDawDB.DefaultOptions
	opts.DirPath = "/tmp/JDawDB"
	db, err := JDawDB.Open(opts)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10000; i++ {
		err = db.Put([]byte("name"), []byte("bitcask"))
		if err != nil {
			panic(err)
		}
	}

	//err = db.Put([]byte("name1"), []byte("name1"))
	//if err != nil {
	//	panic(err)
	//}
	//val, err := db.Get([]byte("name1"))
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("val = ", string(val))

	//err = db.Delete([]byte("name"))
	//if err != nil {
	//	panic(err)
	//}
}
