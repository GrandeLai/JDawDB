package JDawDB

import (
	"JDawDB/data"
	"JDawDB/index"
	"sync"
)

// DB bitcask存储引擎实例
type DB struct {
	options    *Options //文件配置项
	mu         sync.RWMutex
	activeFile *data.DataFile            //当前活跃数据文件
	orderFiles map[uint32]*data.DataFile //旧的数据文件，只读
	indexer    index.Indexer             //内存索引
}

//Put 向数据库中写入K/V数据，Key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//第一步：追加写入到数据文件
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	//第二步：更新内存索引
	if ok := db.indexer.Put(key, pos); !ok {
		return ErrIndexUpdatedFailed
	}
	return nil
}

//Get 根据Key从数据库中读取数据
func (db *DB) Get(key []byte) ([]byte, error) {

	//读数据时需要进行锁的保护
	db.mu.RLock()
	defer db.mu.RUnlock()

	//判断Key的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//从内存索引中获取key对应的LogRecordPos
	logRecordPos := db.indexer.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.orderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//根据偏移量从数据文件中读取数据
	record, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	//如果数据被删除了，则返回nil
	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return record.Value, nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (pos *data.LogRecordPos, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	//判断当前活跃数据文件是否存在，数据库在没有写入时是没有文件生成的
	if db.activeFile == nil {
		if err = db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	//将LogRecord写入到当前活跃数据文件时，需要进行编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	//如果写入的文件大小超过了阈值，则需要切换到新的数据文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//先持久化当前活跃数据文件
		if err = db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.orderFiles[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err = db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	//执行数据写入的操作
	writeOff := db.activeFile.WriteOff
	if err = db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	if db.options.SyncWrites {
		if err = db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//构造LogRecordPos结构体
	pos = &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

//setActiveFile 初始化活跃文件的方法
//访问此方法时需要持有互斥锁
func (db *DB) setActiveFile() error {
	var initialFileId uint32 = 0

	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	//打开新的数据文件
	dataFile, err := data.OpenDataFile(initialFileId, db.options.DirPath)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
