package data

import (
	"github.com/GrandeLai/JDawDB/fio"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(0, os.TempDir(), fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	t.Log(os.TempDir())
}

func TestOpenDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(1231, os.TempDir(), fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello"))
	assert.Nil(t, err)
}

func TestOpenDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(132, os.TempDir(), fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestOpenDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(134, os.TempDir(), fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(551, os.TempDir(), fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	//只有一条record的情况
	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRc1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1.Key, readRc1.Key)
	assert.Equal(t, size1, readSize1)
	t.Log(readSize1)

	//多条record的情况
	rec2 := &LogRecord{
		Key:   []byte("hello1"),
		Value: []byte("world1"),
		Type:  LogRecordNormal,
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRc2, readSize2, err := dataFile.ReadLogRecord(17)
	assert.Nil(t, err)
	assert.Equal(t, rec2.Key, readRc2.Key)
	assert.Equal(t, size2, readSize2)
	t.Log(readSize2)

	//被删除的record应该在文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("hello2"),
		Value: []byte("world2"),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)
	t.Log(size3)

	readRc3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3.Key, readRc3.Key)
	assert.Equal(t, size3, readSize3)
	t.Log(readSize3)
}
