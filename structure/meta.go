package structure

import (
	"encoding/binary"
	"github.com/GrandeLai/JDawDB/utils"
	"math"
)

const (
	maxMetadataSize       = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetadataSize = binary.MaxVarintLen32 * 2
	initialListMark       = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte   //数据类型
	expire   int64  //过期时间
	version  int64  //版本，用于快速删除
	size     uint32 //key下的数据数量
	head     uint64 //list数据结构专有
	tail     uint64 //list数据结构专有
}

// 元数据编码为字节数组
func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetadataSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType

	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

// 将字节数组解码为元数据
func decode(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64
	var tail uint64
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

// HashInternalKey hash实际放入的key
type HashInternalKey struct {
	key     []byte
	version int64 //固定编码存放，占8位
	field   []byte
}

func (hk *HashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)

	//key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	//field
	copy(buf[index:], hk.field)
	return buf
}

// SetInternalKey set实际放入的key
type SetInternalKey struct {
	key     []byte
	version int64 //固定编码存放，占8位
	member  []byte
}

func (sk *SetInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4) //4个字节存储member的长度
	//key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	//member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	//member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

// ListInternalKey list实际放入的key
type ListInternalKey struct {
	key     []byte
	version int64  //固定编码存放，占8位
	index   uint64 //占8位
}

func (lk *ListInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	//key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	//index
	binary.LittleEndian.PutUint32(buf[index:], uint32(lk.index))

	return buf
}

// ZSetInternalKey zset实际放入的key
type ZSetInternalKey struct {
	key     []byte
	version int64 //固定编码存放，占8位
	member  []byte
	score   float64
}

// 因为zset的数据部分有两份，所以需要两个encode方法

func (zk *ZSetInternalKey) encodeWithScore() []byte {
	//需要先将score转换为一个字节数组
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4) //4字节存储member长度

	//key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	//score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	//member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	//member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}

func (zk *ZSetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8+4) //不需要编码score

	//key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	//member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	return buf
}
