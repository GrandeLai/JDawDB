package fio

import "os"

// FileIO 标准系统文件IO实现
type FileIO struct {
	fd *os.File //系统文件描述符
}

func NewFileIOManager(filename string) (*FileIO, error) {
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd}, nil
}

func (fio FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio FileIO) Close() error {
	return fio.fd.Close()
}
