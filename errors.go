package JDawDB

import "errors"

var (
	ErrKeyIsEmpty         = errors.New("key is empty")
	ErrIndexUpdatedFailed = errors.New("fail to update index")
	ErrKeyNotFound        = errors.New("key is not found in JDawDB")
	ErrDataFileNotFound   = errors.New("data file is not found in JDawDB")
	ErrDataFileCorrupted  = errors.New("data file is corrupted")
	ErrExceedMacBatchNum  = errors.New("exceed max batch num")
	ErrMergeInProgress    = errors.New("merge is in progress, please try again later")
	ErrDatabaseIsUsing    = errors.New("database is being used by another process")
)
