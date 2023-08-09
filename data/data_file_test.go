package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(0, os.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	t.Log(os.TempDir())
}

func TestOpenDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(0, os.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello"))
	assert.Nil(t, err)
}

func TestOpenDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(1, os.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestOpenDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(12, os.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}
