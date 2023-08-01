package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

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

//EncodeLogRecord 对LogRecord进行编码，返回byte数组和长度
func EncodeLogRecord(logRecord *LogRecord) (data []byte, size int64) {
	return nil, 0
}
