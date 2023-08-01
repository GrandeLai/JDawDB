package JDawDB

// Options 定义打开文件的配置项
type Options struct {
	DirPath      string
	DataFileSize int64 //数据文件大小
	SyncWrites   bool  //每次写完数据是否都需要安全的持久化
}
