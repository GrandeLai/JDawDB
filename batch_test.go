package JDawDB

import (
	"github.com/GrandeLai/JDawDB/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewWriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-batch")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	t.Log(val1)
	t.Log(err)

	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-batch-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(11), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 校验序列号
	assert.Equal(t, uint64(2), db.seqNo)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOptions
	//dir, _ := os.MkdirTemp("", "JDawDB-batch-3")
	dir := "/tmp/JDawDB-batch-3"
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	t.Log(len(keys))

	//wbOpts := DefaultWriteBatchOptions
	//wbOpts.MaxBatchNum = 10000000
	//wb := db.NewWriteBatch(wbOpts)
	//for i := 0; i < 500000; i++ {
	//	err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	//	assert.Nil(t, err)
	//}
	//err = wb.Commit()
	//assert.Nil(t, err)
}
