package data

//LogRecordPos 描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //日志文件ID
	Offset int64  //偏移量，日志文件中的哪个位置
}
