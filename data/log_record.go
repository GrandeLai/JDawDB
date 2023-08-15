package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// LogRecordHeader的最大值
const maxLogRecordHeaderSize = 5 + 2*binary.MaxVarintLen32

// LogRecordPos 描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //日志文件ID
	Offset int64  //偏移量，日志文件中的哪个位置
}

// LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte        //键
	Value []byte        //值
	Type  LogRecordType //墓碑值，标记该记录是否被删除
}

// LogRecordHeader LogRecord的头部信息
type LogRecordHeader struct {
	crc        uint32        //校验和
	recordType LogRecordType //标识类型
	keySize    uint32        //键的长度
	valueSize  uint32        //值的长度
}

// TransactionLogRecord 暂存事务相关的数据
type TransactionLogRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对LogRecord进行编码，返回byte数组和长度
func EncodeLogRecord(logRecord *LogRecord) (buf []byte, length int64) {
	//初始化一个header部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = logRecord.Type
	var index = 5
	//使用变长类型的编码方式，将key和value的长度写入到header中
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encodeBytes := make([]byte, size)
	//header可能没有使用完，将其拷贝到index部分
	copy(encodeBytes[:index], header[:index])

	copy(encodeBytes[index:], logRecord.Key)
	copy(encodeBytes[index+len(logRecord.Key):], logRecord.Value)

	//计算校验和
	crc := crc32.ChecksumIEEE(encodeBytes[4:])
	binary.LittleEndian.PutUint32(encodeBytes[:4], crc)
	return encodeBytes, int64(size)
}

// DecodeLogRecordHeader 对byte数组进行解码，返回LogRecordHeader对象和长度
func DecodeLogRecordHeader(buf []byte) (header *LogRecordHeader, size int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header = &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	//取出实际的key和value的长度
	keySize, n := binary.Varint(buf[index:]) //返回的是key的长度和实际读取的字节数
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:]) //返回的是value的长度和实际读取的字节数
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// GetRecordCRC 获取byte数组的CRC校验和
func GetRecordCRC(logRecord *LogRecord, buf []byte) uint32 {
	if logRecord == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(buf[:])
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)
	return crc
}
