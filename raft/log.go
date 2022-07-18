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

	entries, err := storage.Entries(firstIndex, endIndex+1)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		raftLog.entries = append(raftLog.entries, entry)
	}

	//if len(raftLog.entries) == 0 {
	//	raftLog.entries = append(raftLog.entries, pb.Entry{}) // dummy entry
	//}

	raftLog.stabled = endIndex
	raftLog.committed = firstIndex - 1
	raftLog.applied = firstIndex - 1
	return raftLog
}

// getEntries [lo, hi)
func (l *RaftLog) getEntries(lo, hi uint64) []pb.Entry {
	if lo < l.FirstIndex()-1 {
		log.Panicf("lo < l.FirstIndex() - 1: %d\n", l.FirstIndex()-1)
	}

	if hi > l.LastIndex()+1 {
		log.Panicf("hi > l.LastIndex()+1: %d\n", l.LastIndex()+1)
	}

	if lo >= hi {
		log.Panicf("lo >= hi\n")
	}

	if len(l.entries) == 0 {
		return nil
	}

	offset := l.entries[0].Index
	if lo < offset {
		panic(ErrCompacted)
	}

	ents := l.entries[lo-offset : hi-offset]
	return ents
}

func (l *RaftLog) findConflictIndex(index uint64, logTerm uint64) uint64 {
	if index > l.LastIndex() {
		panic("index > l.LastIndex()")
	}
	for {
		term, err := l.Term(index)
		if term <= logTerm || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *RaftLog) appendEntries(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	nxtIndex := l.LastIndex() + 1
	if ents[0].Index > nxtIndex {
		log.Panicf("ents[0].Index > nxtIndex, ents: %v, l.entries: %v\n", ents, l.entries)
	}

	if ents[0].Index == nxtIndex || len(l.entries) == 0 {
		l.entries = append(l.entries, ents...)
	} else {
		isTheSame := true // 判断append中重叠的日志是否发生冲突，如果冲突则删除后续原本没被覆盖剩下的日志，否则保留
		for _, e := range ents {
			if e.Index <= l.LastIndex() {
				term, err := l.Term(e.Index)
				if err != nil || term != e.Term {
					isTheSame = false
					break
				}
			}
		}
		offset := l.entries[0].Index
		var leftEntries []pb.Entry
		if ents[len(ents)-1].Index < l.LastIndex() {
			leftEntries = l.entries[ents[len(ents)-1].Index-offset+1:]
		}
		l.entries = l.entries[:ents[0].Index-offset]
		l.entries = append(l.entries, ents...)
		if leftEntries != nil && len(leftEntries) != 0 && isTheSame {
			l.entries = append(l.entries, leftEntries...)
		}

		firstIndex, err := l.storage.FirstIndex()
		if err != nil {
			return l.LastIndex()
		}

		lastIndex, err := l.storage.LastIndex()
		if err != nil {
			return l.LastIndex()
		}
		ens, err := l.storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			return l.LastIndex()
		}

		newStabled := uint64(0)
		for _, e := range ens {
			term, err := l.Term(e.Index)
			if err != nil {
				break
			}
			if term == e.Term {
				newStabled = e.Index
			} else {
				l.stabled = newStabled
				break
			}
		}
	}
	return l.LastIndex()
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

	if l.stabled > l.LastIndex() {
		log.Panicf("l.stabled: %d > l.LastIndex(): %d\n", l.stabled, l.LastIndex())
	}
	offset := l.entries[0].Index
	return l.entries[l.stabled-offset+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//fmt.Printf("applied index: %d, committed index: %d\n", l.applied, l.committed)
	if l.applied > l.committed {
		panic("l.applied > l.committed")
	}

	if l.applied == l.committed {
		return []pb.Entry{}
	}

	entries := l.getEntries(l.applied+1, l.committed+1)
	return entries
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	lastTerm, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}

	return lastTerm
}

func (l *RaftLog) FirstIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}

	firstIndex, err := l.storage.FirstIndex()
	if err == nil {
		return firstIndex
	}

	if len(l.entries) == 0 {
		panic("len(l.entries) == 0\n")
	}
	return l.entries[0].Index + 1
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		if l.pendingSnapshot != nil {
			return l.pendingSnapshot.Metadata.Index
		} else {
			lastIndex, err := l.storage.LastIndex()
			if err != nil {
				panic(err)
			}
			return lastIndex
		}
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	dummyIndex := l.FirstIndex() - 1
	lastIndex := l.LastIndex()
	if i < dummyIndex || i > lastIndex {
		return 0, errors.New("i index out of the entries")
	}

	if l.pendingSnapshot != nil {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
	}

	if i == 0 {
		return 0, nil
	}

	if len(l.entries) == 0 {
		panic("len(l.entries) == 0\n")
	}

	offset := l.entries[0].Index
	return l.entries[i-offset].Term, nil
}
