package JDawDB

import (
	"errors"
	"github.com/GrandeLai/JDawDB/data"
	"github.com/GrandeLai/JDawDB/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask存储引擎实例
type DB struct {
	options    *Options //文件配置项
	mu         *sync.RWMutex
	fileIds    []int                     //有序的数据文件ID列表
	activeFile *data.DataFile            //当前活跃数据文件
	orderFiles map[uint32]*data.DataFile //旧的数据文件，只读
	indexer    index.Indexer             //内存索引
}

// Open 打开bitcask存储引擎
func Open(options Options) (db *DB, err error) {
	//对传入配置项进行校验
	if err = checkOptions(options); err != nil {
		return nil, err
	}
	//对传递的目录进行校验，如果不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化DB实例结构体
	db = &DB{
		options:    &options,
		mu:         new(sync.RWMutex),
		orderFiles: make(map[uint32]*data.DataFile),
		indexer:    index.NewIndexer(options.IndexType),
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("data file size must be greater than 0")
	}
	return nil
}

// Put 向数据库中写入K/V数据，Key不能为空
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

//Delete 根据key删除对应的数据
func (db *DB) Delete(key []byte) error {
	//判断key的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//检查key是否存在
	if pos := db.indexer.Get(key); pos == nil {
		return ErrKeyNotFound
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	//写入到数据文件中
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	//从内存索引中删除
	ok := db.indexer.Delete(key)
	if !ok {
		return ErrIndexUpdatedFailed
	}
	return nil
}

// Get 根据Key从数据库中读取数据
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
	record, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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

// setActiveFile 初始化活跃文件的方法
// 访问此方法时需要持有互斥锁
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

// loadDataFiles 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	//遍历目录下的文件，获取.data文件的文件名
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//获取文件名中的文件ID
			fileId, err := strconv.Atoi(strings.Split(entry.Name(), ".")[0])
			if err != nil {
				return ErrDataFileCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	//对文件ID进行排序
	sort.Ints(fileIds)
	db.fileIds = fileIds

	//遍历文件ID，依次打开数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(uint32(fid), db.options.DirPath)
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 { //最后一个文件说明是活跃文件
			db.activeFile = dataFile
		} else {
			db.orderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// loadIndexFromDataFiles 遍历文件中所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	for i, fid := range db.fileIds {
		var fileId uint32 = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.orderFiles[fileId]
		}

		var offset int64 = 0
		//循环处理文件的内容
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//如果读取到文件末尾，则退出循环
				if err == io.EOF {
					break
				}
				return err
			}

			//构造内存索引并且保存到内存索引中
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			var ok bool
			//判断LogRecord的类型，如果是删除操作，则从内存索引中删除
			if logRecord.Type == data.LogRecordDeleted {
				ok = db.indexer.Delete(logRecord.Key)
			} else {
				ok = db.indexer.Put(logRecord.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdatedFailed
			}
			//递增偏移量，下次循环从下一个位置开始读取
			offset += size
		}

		//如果判断到是最后一个活跃文件，需要维护writeOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}
