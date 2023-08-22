package data

import (
	"errors"
	"fmt"
	"github.com/GrandeLai/JDawDB/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value,log record may be corrupted")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        //文件ID
	WriteOff  int64         //写入偏移量
	IoManager fio.IOManager //IO读写管理器
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(fileId uint32, dirPath string) (file *DataFile, err error) {
	fileName := GetDataFileName(dirPath, fileId)
	return NewDataFile(fileName, fileId)
}

// OpenHintFile 打开hint索引文件
func OpenHintFile(dirPath string) (hintFile *DataFile, err error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return NewDataFile(fileName, 0)
}

// OpenMergeFinishedFile 打开一个表示merge完成的文件
func OpenMergeFinishedFile(dirPath string) (mergeFile *DataFile, err error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return NewDataFile(fileName, 0)
}

// OpenSeqNoFile 打开一个存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (seqNoFile *DataFile, err error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return NewDataFile(fileName, 0)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func NewDataFile(fileName string, fileId uint32) (*DataFile, error) {
	//获取IOManager对象
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 根据偏移量读取数据
func (file *DataFile) ReadLogRecord(offset int64) (logRecord *LogRecord, size int64, err error) {
	//判断读取的时候，是否超过了文件的大小，否则的话，只读取到文件的末尾即可
	fileSize, err := file.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}
	//读取Header信息
	headerBuf, err := file.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	//对header进行解码
	header, headerSize := DecodeLogRecordHeader(headerBuf)
	//如果header为空，说明读取到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	//如果读取到的校验值和kv长度都为0，说明读到了文件末尾
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = keySize + valueSize + headerSize

	logRecord = &LogRecord{
		Type: header.recordType,
	}
	//读取LogRecord中实际的key和value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := file.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		//分离出key和value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	//校验数据的有效性
	crc := GetRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil

}

// 定义数据写入的方法
func (file *DataFile) Write(data []byte) (err error) {
	n, err := file.IoManager.Write(data)
	if err != nil {
		return err
	}
	file.WriteOff += int64(n)
	return
}

// Sync 将数据文件的内容持久化到磁盘
func (file *DataFile) Sync() (err error) {
	return file.IoManager.Sync()
}

// Close 关闭数据文件
func (file *DataFile) Close() (err error) {
	return file.IoManager.Close()
}

// 调用IOManager的Read方法读取指定位置，指定大小的数据，返回byte数组
func (file *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = file.IoManager.Read(b, offset)
	return
}

// WriteHintRecord 写pos到hint索引文件
func (file *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	logRecord := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(logRecord)
	return file.Write(encRecord)
}
