package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	dirSize, err := DirSize(dir)
	assert.Nil(t, err)
	assert.True(t, dirSize > 0)
}

func TestAvailableDiskSizeOnLinux(t *testing.T) {
	size, err := AvailableDiskSizeOnLinux()
	assert.Nil(t, err)
	assert.True(t, size > 0)
}

//func TestAvailableDiskSizeOnWin(t *testing.T) {
//	size, err := AvailableDiskSizeOnWin()
//	assert.Nil(t, err)
//	assert.True(t, size > 0)
//}
