package JDawDB

import (
	"github.com/GrandeLai/JDawDB/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	// 定义merge文件夹的名称
	mergeDirName = "-merge"
	//记录在标识merge完成文件中的logRecord的key
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成hint文件
func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	if db.isMerging {
		return ErrMergeInProgress
	}
	db.isMerging = true

	defer func() {
		db.isMerging = false
	}()
	//持久化当前活跃的数据文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	//将当前活跃的数据文件加入到旧的数据文件列表中
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	//打开一个新的数据文件
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	//获取最近一个没有参与merge的数据文件
	nonMergeFileId := db.activeFile.FileId

	//取出所以旧的数据文件，也就是需要merge的文件
	mergeFiles := make([]*data.DataFile, 0, len(db.olderFiles))
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	//从小到大排序mergeFiles后进行merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := db.getMergePath()
	//判断目录是否存在，说明发生过merge，删除目录
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	//新建一个mergePath的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	//新打开一个db实例，用于merge
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false //因为merge不可能都成功，每次都sync可能会导致merge变慢
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	//打开一个hint文件，存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	//遍历处理每个旧的数据文件
	for _, file := range mergeFiles {
		var offset int64
		for {
			logRecord, size, err := file.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := ParseLogRecordKeyWithSeqNo(logRecord.Key)
			logRecordPos := db.indexer.Get(realKey)
			//和内存中的索引位置进行比较，如果有就重写
			if logRecordPos != nil &&
				file.FileId == logRecordPos.Fid &&
				logRecordPos.Offset == offset {
				//重写到新的数据文件中，要清除事务标记
				logRecord.Key = LogRecordKeyWithSeqNo(realKey, NonTxnSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//将位置索引写到Hint文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}
	//sync保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	//写标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	//小于nonMergeFileId的数据文件都已经merge完成
	encRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 获取需要merge的文件的目录，比如当前文件夹是/tmp/JDawDB,就生成/tmp/JDawDB-merge
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath)) //clean是为了去掉最后的/
	//获取目录的父目录
	baseDir := path.Base(db.options.DirPath)
	return path.Join(dir, baseDir+mergeDirName)
}

// 启动时对merge目录进行处理
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	//读取merge目录
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	//检查是否有merge完成的标识文件
	var mergeFinishedSign bool
	//merge文件的文件名数组
	var mergeFileNames []string
	for _, dirEntry := range dirEntries {
		if dirEntry.Name() == data.MergeFinishedFileName {
			mergeFinishedSign = true
			break
		}
		if dirEntry.Name() == data.SeqNoFileName {
			continue
		}
		mergeFileNames = append(mergeFileNames, dirEntry.Name())
	}
	if !mergeFinishedSign {
		return nil
	}

	//merge完成，删除旧的数据文件，用merge后的数据文件替换
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	//删除比nonMergeFileId小的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	//将merge后的数据文件移动过来
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

// 读取最近一个没有参与merge的数据文件的id
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	//因为只有一条数据所以offset为0
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 从hint文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	//hint文件不存在，直接返回
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	var offset int64
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.indexer.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
