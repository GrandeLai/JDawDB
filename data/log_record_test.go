package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	//正常情况
	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}
	data1, n1 := EncodeLogRecord(rec1)
	t.Log(data1)
	assert.NotNil(t, data1)
	t.Log(n1)
	assert.Greater(t, n1, int64(5))

	//value为空的情况
	rec2 := &LogRecord{
		Key:  []byte("hello"),
		Type: LogRecordNormal,
	}
	data2, n2 := EncodeLogRecord(rec2)
	t.Log(data2)
	assert.NotNil(t, data2)
	t.Log(n2)
	assert.Greater(t, n2, int64(5))

	//delete的情况
	rec3 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordDeleted,
	}
	data3, n3 := EncodeLogRecord(rec3)
	t.Log(data3)
	assert.NotNil(t, data3)
	t.Log(n3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{11, 213, 225, 252, 0, 5, 5}
	header1, size1 := DecodeLogRecordHeader(headerBuf1)
	t.Log(header1)
	t.Log(size1)
	//assert.NotNil(t, header1)
	//assert.Equal(t, int64(7), size1)
	//assert.Equal(t, uint32(5), header1.keySize)

	headerBuf2 := []byte{85, 161, 40, 199, 0, 5, 0}
	header2, size2 := DecodeLogRecordHeader(headerBuf2)
	t.Log(header2)
	t.Log(size2)

	headerBuf3 := []byte{142, 12, 119, 33, 1, 5, 5}
	header3, size3 := DecodeLogRecordHeader(headerBuf3)
	t.Log(header3)
	t.Log(size3)
}

func TestGetRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{11, 213, 225, 252, 0, 5, 5}
	crc1 := GetRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(4242658571), crc1)

	rec2 := &LogRecord{
		Key:  []byte("hello"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{85, 161, 40, 199, 0, 5, 0}
	crc2 := GetRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(3341328725), crc2)

	rec3 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{142, 12, 119, 33, 1, 5, 5}
	crc3 := GetRecordCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(561450126), crc3)
}
