package fio

// DataFilePerm 定义文件打开权限
const DataFilePerm = 0644

// IOManager 抽象IO接口，后续加入的数据结构可以直接实现该接口
type IOManager interface {
	// Read 从指定位置读取数据
	Read([]byte, int64) (int, error)
	// Write 从指定位置写入字节数组到文件
	Write([]byte) (int, error)
	// Sync 同步数据到磁盘
	Sync() error
	// Close 关闭文件
	Close() error
	// Size 获取文件大小
	Size() (int64, error)
}

// NewIOManager 根据类型初始化IOManager，目前只有FileIO
func NewIOManager(filename string) (IOManager, error) {
	return NewFileIOManager(filename)
}
