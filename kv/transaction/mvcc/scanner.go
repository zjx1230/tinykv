package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	searchKey []byte
	txn       *MvccTxn
	iter      engine_util.DBIterator
	isEnd     bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		searchKey: startKey,
		txn:       txn,
		iter:      txn.Reader.IterCF(engine_util.CfWrite),
		isEnd:     false,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.isEnd {
		return nil, nil, nil
	}

	scan.iter.Seek(EncodeKey(scan.searchKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	item := scan.iter.Item()
	key := DecodeUserKey(item.KeyCopy(nil))
	if bytes.Compare(key, scan.searchKey) != 0 {
		scan.searchKey = key
		return scan.Next()
	}

	// adjust the iter
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			scan.isEnd = true
			break
		}
		nextKey := DecodeUserKey(scan.iter.Item().KeyCopy(nil))
		if bytes.Compare(nextKey, scan.searchKey) != 0 {
			scan.searchKey = nextKey
			break
		}
	}

	writeData, err := item.ValueCopy(nil)
	if err != nil {
		return nil, nil, err
	}

	write, err := ParseWrite(writeData)
	if err != nil {
		return nil, nil, err
	}

	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}

	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}
