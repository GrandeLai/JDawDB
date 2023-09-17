package structure

import (
	"github.com/GrandeLai/JDawDB"
	"github.com/GrandeLai/JDawDB/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestDataStructure_Get(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-redis-get")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	err = ds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = ds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := ds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := ds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = ds.Get(utils.GetTestKey(33))
	assert.Equal(t, JDawDB.ErrKeyNotFound, err)
}

func TestDataStructure_Del_Type(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-redis-del-type")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	// del
	err = ds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = ds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	// type
	typ, err := ds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = ds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = ds.Get(utils.GetTestKey(1))
	assert.Equal(t, JDawDB.ErrKeyNotFound, err)
}

func TestDataStructure_HGet(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-hget")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := ds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandomValue(100)
	ok2, err := ds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandomValue(100)
	ok3, err := ds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	val1, err := ds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)

	val2, err := ds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, val2)

	_, err = ds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, JDawDB.ErrKeyNotFound, err)
}

func TestDataStructure_HDel(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-hdel")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	del1, err := ds.HDel(utils.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := ds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandomValue(100)
	ok2, err := ds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandomValue(100)
	ok3, err := ds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := ds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)
}

func TestDataStructure_SIsMember(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-sismember")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	ok, err := ds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = ds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = ds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = ds.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = ds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = ds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = ds.SIsMember(utils.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestDataStructure_SRem(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-srem")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	ok, err := ds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = ds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = ds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = ds.SRem(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = ds.SRem(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = ds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestDataStructure_LPop(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-lpop")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	res, err := ds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = ds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = ds.LPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := ds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = ds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = ds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestDataStructure_RPop(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-rpop")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	res, err := ds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = ds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = ds.RPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := ds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = ds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = ds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestDataStructure_ZScore(t *testing.T) {
	opts := JDawDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "JDawDB-zset")
	opts.DirPath = dir
	ds, err := NewDataStructure(opts)
	assert.Nil(t, err)

	ok, err := ds.ZAdd(utils.GetTestKey(1), 113, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = ds.ZAdd(utils.GetTestKey(1), 333, []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = ds.ZAdd(utils.GetTestKey(1), 98, []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	score, err := ds.ZScore(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, float64(333), score)
	score, err = ds.ZScore(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(98), score)
}
