package index

import (
	"github.com/GrandeLai/JDawDB/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos1 := art.Get([]byte("key-1"))
	t.Logf("pos: %v", pos1)
	assert.NotNil(t, pos1)

	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos2 := art.Get([]byte("key-2"))
	t.Logf("pos: %v", pos2)
	assert.NotNil(t, pos2)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 12, Offset: 12})
	pos3 := art.Get([]byte("key-1"))
	t.Logf("pos: %v", pos3)
	assert.NotNil(t, pos3)

}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res := art.Delete([]byte("key-2"))
	assert.False(t, res)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos1 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos1)

	res1 := art.Delete([]byte("key-1"))
	assert.True(t, res1)

	pos1 = art.Get([]byte("key-1"))
	assert.Nil(t, pos1)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
