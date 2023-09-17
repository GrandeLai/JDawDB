package structure

import "errors"

func (ds *DataStructure) Del(key []byte) error {
	return ds.db.Delete(key)
}

func (ds *DataStructure) Type(key []byte) (DataType, error) {
	encValue, err := ds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}
	// 第一个字节就是类型
	return encValue[0], nil
}

func (ds *DataStructure) Close() error {
	return ds.db.Close()
}
