package benchmark

import (
	"github.com/GrandeLai/JDawDB"
	"github.com/GrandeLai/JDawDB/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

//Benchmark_Put-4            15853             85075 ns/op            4636 B/op             10 allocs/op
//Benchmark_Put-4            执行次数            每次执行的耗时           每次内存分配了4638B      每次操作分配了10次内存

var db *JDawDB.DB

func init() {
	// 初始化用于基准测试的存储引擎
	options := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-bench")
	options.DirPath = dir

	var err error
	db, err = JDawDB.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()   //计时器重置
	b.ReportAllocs() //打印出每个测试内存分配的情况

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != JDawDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
