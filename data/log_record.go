package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

//LogRecordHeader的最大值
const maxLogRecordHeaderSize = 5 + 2*binary.MaxVarintLen32

//LogRecordPos 描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //日志文件ID
	Offset int64  //偏移量，日志文件中的哪个位置
}

//LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte        //键
	Value []byte        //值
	Type  LogRecordType //墓碑值，标记该记录是否被删除
}

//LogRecordHeader LogRecord的头部信息
type LogRecordHeader struct {
	crc        uint32        //校验和
	recordType LogRecordType //标识类型
	keySize    uint32        //键的长度
	valueSize  uint32        //值的长度
}

//EncodeLogRecord 对LogRecord进行编码，返回byte数组和长度
func EncodeLogRecord(logRecord *LogRecord) (buf []byte, size int64) {
	return nil, 0
}

//DecodeLogRecordHeader 对byte数组进行解码，返回LogRecordHeader对象和长度
func DecodeLogRecordHeader(buf []byte) (header *LogRecordHeader, size int64) {
	return nil, 0
}

//GetRecordCRC 获取byte数组的CRC校验和
func GetRecordCRC(logRecord *LogRecord, buf []byte) uint32 {
	return 0
}
