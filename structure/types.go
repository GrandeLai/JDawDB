package structure

import (
	"encoding/binary"
	"errors"
	"github.com/GrandeLai/JDawDB"
	"github.com/GrandeLai/JDawDB/utils"
	"time"
)

type DataType = byte

var ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

const (
	String DataType = iota
	Hash
	Set
	List
	ZSet
)

// DataStructure nosql的数据结构服务
type DataStructure struct {
	db *JDawDB.DB
}

// NewDataStructure 初始化DataStructure
func NewDataStructure(opt JDawDB.Options) (*DataStructure, error) {
	db, err := JDawDB.Open(opt)
	if err != nil {
		panic(err)
	}
	return &DataStructure{
		db: db,
	}, nil
}

// -----------------String数据结构-----------------

func (ds *DataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	//编码value：type+expire+payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	//调用存储引擎接口写入
	return ds.db.Put(key, encValue)
}

func (ds *DataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := ds.db.Get(key)
	if err != nil {
		return nil, err
	}

	//解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	//判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return encValue[index:], nil
}

// -----------------Hash数据结构-----------------

func (ds *DataStructure) HSet(key, field, value []byte) (bool, error) {
	//先查找元数据
	meta, err := ds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	//构造has实际放入的key
	hk := &HashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	//先查找是否存在
	var exist = true
	if _, err = ds.db.Get(encKey); err == JDawDB.ErrKeyNotFound {
		exist = false
	}

	//初始化writebatch，保证原子性
	writebatch := ds.db.NewWriteBatch(JDawDB.DefaultWriteBatchOptions)

	//不存在，说明需要增加size，更新元数据
	if !exist {
		meta.size++
		_ = writebatch.Put(key, meta.encode())
	}
	//更新数据
	_ = writebatch.Put(encKey, value)
	if err = writebatch.Commit(); err != nil {
		return false, err
	}
	return !exist, nil

}

func (ds *DataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := ds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &HashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return ds.db.Get(hk.encode())
}

func (ds *DataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := ds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &HashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err = ds.db.Get(encKey); err == JDawDB.ErrKeyNotFound {
		exist = false
	}

	if exist {
		writebatch := ds.db.NewWriteBatch(JDawDB.DefaultWriteBatchOptions)
		meta.size--
		//更新元数据
		_ = writebatch.Put(key, meta.encode())
		//
		_ = writebatch.Delete(encKey)
		if err = writebatch.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// -----------------Set数据结构-----------------

func (ds *DataStructure) SAdd(key, member []byte) (bool, error) {
	//查找元数据
	meta, err := ds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	//构造一个数据部分的key
	sk := &SetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = ds.db.Get(sk.encode()); err == JDawDB.ErrKeyNotFound {
		//不存在就更新
		writebatch := ds.db.NewWriteBatch(JDawDB.DefaultWriteBatchOptions)
		meta.size++
		_ = writebatch.Put(key, meta.encode())
		_ = writebatch.Put(sk.encode(), nil)
		if err = writebatch.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// SIsMember 判断传入的member是否是这个key的成员
func (ds *DataStructure) SIsMember(key, member []byte) (bool, error) {
	//查找元数据
	meta, err := ds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		//说明这个key下没有数据的
		return false, err
	}

	//构造一个数据部分的key
	sk := &SetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = ds.db.Get(sk.encode())
	if err != nil && err != JDawDB.ErrKeyNotFound {
		return false, err
	}
	if err == JDawDB.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (ds *DataStructure) SRem(key, member []byte) (bool, error) {
	//查找元数据
	meta, err := ds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		//说明这个key下没有数据的
		return false, err
	}

	//构造一个数据部分的key
	sk := &SetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = ds.db.Get(sk.encode()); err == JDawDB.ErrKeyNotFound {
		return false, nil
	}

	//不存在就更新
	writebatch := ds.db.NewWriteBatch(JDawDB.DefaultWriteBatchOptions)
	meta.size--
	_ = writebatch.Put(key, meta.encode())
	_ = writebatch.Delete(sk.encode())
	if err = writebatch.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

// -----------------List数据结构-----------------

func (ds *DataStructure) LPush(key, element []byte) (uint32, error) {
	return ds.pushInner(key, element, true)
}

func (ds *DataStructure) RPush(key, element []byte) (uint32, error) {
	return ds.pushInner(key, element, false)
}

func (ds *DataStructure) LPop(key []byte) ([]byte, error) {
	return ds.popInner(key, true)
}

func (ds *DataStructure) RPop(key []byte) ([]byte, error) {
	return ds.popInner(key, false)
}

// LPush和RPush时处理head和tail
func (ds *DataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	//查找元数据
	meta, err := ds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	//构造数据部分的key
	lk := &ListInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	//更新元数据和数据部分
	writebatch := ds.db.NewWriteBatch(JDawDB.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = writebatch.Put(key, meta.encode())
	_ = writebatch.Put(lk.encode(), element)
	if err = writebatch.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

// LPop和RPop时处理head和tail
func (ds *DataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	//查找元数据
	meta, err := ds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	//构造数据部分的key
	lk := &ListInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := ds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = ds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, err
}

// -----------------ZSet数据结构-----------------

func (ds *DataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	//查找元数据
	meta, err := ds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	//构造数据部分的key
	zk := &ZSetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	//查看是否已经存在
	var exist = true
	value, err := ds.db.Get(zk.encodeWithMember())
	if err != nil && err != JDawDB.ErrKeyNotFound {
		return false, err
	}
	if err == JDawDB.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	//需要更新元数据
	writebatch := ds.db.NewWriteBatch(JDawDB.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = writebatch.Put(key, meta.encode())
	}
	if exist {
		//需要删除旧的key
		oldKey := &ZSetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = writebatch.Delete(oldKey.encodeWithScore())
	}
	//更新数据部分
	_ = writebatch.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = writebatch.Put(zk.encodeWithScore(), nil)
	if err = writebatch.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (ds *DataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := ds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &ZSetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := ds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

func (ds *DataStructure) findMetadata(key []byte, dataType DataType) (*metadata, error) {
	metaBuf, err := ds.db.Get(key)
	if err != nil && err != JDawDB.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true //除了本身不存在，如果过期了也是不存在，需要标识
	if err == JDawDB.ErrKeyNotFound {
		exist = false
	} else {
		meta = decode(metaBuf)
		//判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		if meta.expire > 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		//不存在，需要初始化
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
