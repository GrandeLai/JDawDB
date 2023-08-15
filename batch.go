package JDawDB

import (
	"encoding/binary"
	"github.com/GrandeLai/JDawDB/data"
	"sync"
	"sync/atomic"
)

// 非事务常量
const NonTxnSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch 原子批量写数据，类似事务的功能
type WriteBatch struct {
	mu            *sync.RWMutex
	db            *DB
	opts          WriteBatchOptions
	pendingWrites map[string]*data.LogRecord // 待写入的数据
}

// NewWriteBatch 初始化WriteBatch
func (db *DB) NewWriteBatch(opt WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		mu:            new(sync.RWMutex),
		db:            db,
		opts:          opt,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写入数据
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	//暂存LogRecord
	wb.pendingWrites[string(key)] = &data.LogRecord{
		Key:   key,
		Value: value,
	}
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	//数据不存在就直接返回
	logRecordPos := wb.db.indexer.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}
	//暂存LogRecord
	wb.pendingWrites[string(key)] = &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	return nil
}

// Commit 提交事务，将暂存的数据写到数据文件，并更新索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.opts.MaxBatchNum {
		return ErrExceedMacBatchNum
	}

	//加db的锁保证事务提交的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	//获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	//开始写数据到数据文件
	positions := make(map[string]*data.LogRecordPos)
	for _, logRecord := range wb.pendingWrites {

		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			//需要将key进行简单编码，加上seqNo
			Key:   LogRecordKeyWithSeqNo(logRecord.Key, seqNo),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return err
		}
		positions[string(logRecord.Key)] = logRecordPos
	}

	//写一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  LogRecordKeyWithSeqNo(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//根据配置决定是否立即刷盘
	if wb.opts.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//更新对应的内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.indexer.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.indexer.Delete(record.Key)
		}
	}

	//清空暂存的数据，方便下一次commit
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// LogRecordKeyWithSeqNo 将key和seqNo进行编码
func LogRecordKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, len(key)+n)
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// ParseLogRecordKeyWithSeqNo 解析logRecord的key，获取实际的key和seqNo
func ParseLogRecordKeyWithSeqNo(key []byte) (realKey []byte, seqNo uint64) {
	seqNo, n := binary.Uvarint(key)
	return key[n:], seqNo
}
