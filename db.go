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
	olderFiles map[uint32]*data.DataFile //旧的数据文件，只读
	indexer    index.Indexer             //内存索引
	seqNo      uint64                    //事务序列号，全局递增
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
		olderFiles: make(map[uint32]*data.DataFile),
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
		Key:   LogRecordKeyWithSeqNo(key, NonTxnSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//第一步：追加写入到数据文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	//第二步：更新内存索引
	if ok := db.indexer.Put(key, pos); !ok {
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

	return db.GetValueByPosition(logRecordPos)
}

// Delete 根据key删除对应的数据
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
		Key:  LogRecordKeyWithSeqNo(key, NonTxnSeqNo),
		Type: data.LogRecordDeleted,
	}
	//写入到数据文件中
	_, err := db.appendLogRecordWithLock(logRecord)
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

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	//关闭当前活跃的数据文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	//关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// ListKeys 获取数据文件中所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.indexer.Iterator(false)
	keys := make([][]byte, db.indexer.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取数据文件中所有的key，并按照传入的方法执行相对应的操作，返回false时停止遍历
func (db *DB) Fold(callback func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.indexer.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.GetValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !callback(iterator.Key(), value) {
			//返回false时，停止遍历
			break
		}
	}
	return nil
}

// GetValueByPosition 根据索引信息LogRecordPos从文件中读取value值
func (db *DB) GetValueByPosition(pos *data.LogRecordPos) ([]byte, error) {

	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//根据偏移量从数据文件中读取数据
	record, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	//如果数据被删除了，则返回nil
	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return record.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (pos *data.LogRecordPos, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (pos *data.LogRecordPos, err error) {

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

		db.olderFiles[db.activeFile.FileId] = db.activeFile

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
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// loadIndexFromDataFiles 遍历文件中所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	//定义更新内存索引的函数
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		//判断LogRecord的类型，如果是删除操作，则从内存索引中删除
		if typ == data.LogRecordDeleted {
			ok = db.indexer.Delete(key)
		} else {
			ok = db.indexer.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	//暂存事务数据，判断对应事务no是否可以提交，如果可以提交，则将事务中的数据列表更新到内存索引中
	transactionRecords := make(map[uint64][]*data.TransactionLogRecord)
	var currentSeqNo = NonTxnSeqNo

	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
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

			//解析key，获取实物序列号
			realKey, seqNo := ParseLogRecordKeyWithSeqNo(logRecord.Key)
			if seqNo == NonTxnSeqNo {
				//非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//事务完成，需要将事务中的所有操作更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, logRecordPos)
					}
					delete(transactionRecords, seqNo)
				} else {
					//暂未判断事务是否提交，将事务中的操作暂存到transactionRecords中
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionLogRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}
			//更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			//递增偏移量，下次循环从下一个位置开始读取
			offset += size
		}

		//如果判断到是最后一个活跃文件，需要维护writeOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	//更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}
