package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap 内存文件映射，目前只用来读数据
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化 MMap Io
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt}, nil
}

func (m *MMap) Read(p []byte, off int64) (n int, err error) {
	return m.readerAt.ReadAt(p, off)
}

func (m *MMap) Write(p []byte) (n int, err error) {
	panic("not implemented")
}

func (m *MMap) Sync() error {
	panic("not implemented")
}

func (m *MMap) Close() error {
	return m.readerAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
