// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"github.com/juju/errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	raftLog := &RaftLog{
		storage: storage,
		entries: make([]pb.Entry, 0),
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Printf("[newLog] firstIndex: %v\n", err)
		return raftLog
	}

	endIndex, err := storage.LastIndex()
	if err != nil {
		log.Printf("[newLog] endIndex, err: %v\n", err)
		return raftLog
	}

	newEntries, err := storage.Entries(firstIndex, endIndex+1)
	if err != nil {
		log.Printf("[newLog] storage.Entries, err: %v\n", err)
		return raftLog
	}
	raftLog.entries = append(raftLog.entries, newEntries...)
	raftLog.stabled = raftLog.LastIndex()
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}

	if l.stabled == 0 {
		return l.entries
	}

	unStableIndex := l.GetRealIndex(l.stabled)
	if unStableIndex+1 >= uint64(len(l.entries)) {
		return []pb.Entry{}
	}
	return l.entries[unStableIndex+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	fmt.Printf("applied index: %d, committed index: %d\n", l.applied, l.committed)
	if len(l.entries) == 0 {
		return nil
	}
	realApplied := l.GetRealIndex(l.applied)
	realCommitted := l.GetRealIndex(l.committed)
	if l.applied == 0 { // not applied
		return l.entries[realApplied : realCommitted+1]
	}

	if realApplied >= realCommitted {
		return nil
	}
	return l.entries[realApplied+1 : realCommitted+1]
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}

	index := l.GetRealIndex(i)
	if index < uint64(len(l.entries)) {
		return l.entries[index].Term, nil
	}

	return 0, errors.New("i index out of the entries")
}

// GetRealIndex get real index of the array of the entries
func (l *RaftLog) GetRealIndex(index uint64) uint64 {
	// TODO
	if len(l.entries) == 0 || index == 0 {
		return 0
	}
	return index - l.entries[0].Index
}

func (l *RaftLog) GetDataByIndex(index uint64) []byte {
	realIndex := l.GetRealIndex(index)
	if realIndex >= uint64(len(l.entries)) {
		return nil
	}
	return l.entries[realIndex].Data
}
