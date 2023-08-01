package data

import "JDawDB/fio"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32         //文件ID
	WriteOff  int64          //写入偏移量
	IoManager *fio.IOManager //IO读写管理器
}

//根据偏移量读取数据
func (file *DataFile) ReadLogRecord(offset int64) (logRecord *LogRecord, err error) {
	return nil, nil
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(fileId uint32, filePath string) (file *DataFile, err error) {
	return
}

//定义数据写入的方法
func (file *DataFile) Write(data []byte) (err error) {
	return
}

// Sync 将数据文件的内容持久化到磁盘
func (file *DataFile) Sync() (err error) {
	return
}
