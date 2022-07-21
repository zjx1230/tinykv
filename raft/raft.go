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
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct { // index = the real index of raftLog Entries + 1
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	peers []uint64

	// get accept vote num
	votesNum int

	electionTick int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).
	raft := &Raft{
		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTick:     c.ElectionTick,
		RaftLog:          newLog(c.Storage),
		peers:            c.peers,
		votes:            make(map[uint64]bool, 0),
		Prs:              make(map[uint64]*Progress, 0),
	}

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	if !IsEmptyHardState(hardState) {
		raft.Vote = hardState.Vote
		raft.Term = hardState.Term
		raft.RaftLog.committed = hardState.Commit
	}

	if raft.RaftLog.committed > 0 && len(raft.RaftLog.entries) == 0 {
		raft.RaftLog.entries = append(raft.RaftLog.entries, pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      raft.Term,
			Index:     raft.RaftLog.committed,
		})
	}

	if confState.Nodes != nil && len(confState.Nodes) != 0 {
		if raft.peers == nil {
			raft.peers = make([]uint64, 0)
		}

		for _, id := range confState.Nodes {
			raft.peers = append(raft.peers, id)
		}
	}

	for _, peer := range raft.peers {
		raft.Prs[peer] = &Progress{
			Match: 0,
			Next:  raft.RaftLog.LastIndex() + 1,
		}
	}
	raft.electionTimeout = raft.electionTick + rand.Intn(10000)%c.ElectionTick
	raft.RaftLog.applied = c.Applied
	return raft
}

func (r *Raft) getSoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) getHardState() *pb.HardState {
	return &pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) GetPeers() []uint64 {
	return r.peers
}

func (r *Raft) GetHardState() pb.HardState {
	hardState, _, err := r.RaftLog.storage.InitialState()
	if err != nil {
		panic(err)
	}
	return hardState
}

func (r *Raft) appendEntries(ents ...pb.Entry) uint64 {
	lastIndex := r.RaftLog.LastIndex()
	for i := range ents {
		if ents[i].Index == 0 {
			ents[i].Index = lastIndex + uint64(i) + 1
		}

		if ents[i].Term == 0 {
			ents[i].Term = r.Term
		}
	}

	lastIndex = r.RaftLog.appendEntries(ents...)
	r.Prs[r.id].Next = max(r.Prs[r.id].Next, lastIndex+1)
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1
	return lastIndex
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}

	var newEntries []pb.Entry
	if r.Prs[to].Next < r.RaftLog.LastIndex()+1 {
		newEntries = r.RaftLog.getEntries(r.Prs[to].Next, r.RaftLog.LastIndex()+1)
	}

	logTerm, err := r.RaftLog.Term(r.Prs[to].Next - 1)
	if err != nil {
		panic(err)
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   r.Prs[to].Next - 1,
		Entries: convertReferenceEntryArray(newEntries),
		Commit:  r.RaftLog.committed,
	})
	//fmt.Printf("newEntries: %v\n r.msg: %v\n", newEntries, r.msgs[len(r.msgs)-1])
	return true
}

func (r *Raft) sendHeartBeat() {
	for _, peer := range r.peers {
		if r.id == peer {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  r.RaftLog.committed,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed%r.heartbeatTimeout == 0 {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed%r.electionTimeout == 0 {
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}

}

func (r *Raft) sendElection() {
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      peer,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastTerm(),
			Index:   r.RaftLog.LastIndex(),
			MsgType: pb.MessageType_MsgRequestVote,
		})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Vote = None
	r.votes = make(map[uint64]bool, 0)
	r.votesNum = 0
	r.electionTimeout = r.electionTick + rand.Intn(10000)%r.electionTick
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Lead = None
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool, 0)
	r.votes[r.id] = true
	r.votesNum = 1
	r.State = StateCandidate
	r.electionTimeout = r.electionTick + rand.Intn(10000)%r.electionTick
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	//log.Debugf("become Leader\n")
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None
	r.votes = make(map[uint64]bool, 0)
	r.votesNum = 0

	r.appendEntries(pb.Entry{Data: nil})
	if len(r.peers) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	//fmt.Printf("become Leader, id : %d\n", r.id)
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		//fmt.Printf("id : %d, r.Prs[id].Next: %d\n", id, r.Prs[id].Next)
		r.sendAppend(id)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	r.Term = max(r.Term, m.Term)
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgRequestVote:
		r.handleMsgRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgRequestVote:
		r.handleMsgRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleMsgRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleMsgRequestVote(m)
	case pb.MessageType_MsgBeat:
		r.sendHeartBeat()
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleMsgHeartbeatResponse(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleMsgAppendResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
}

func (r *Raft) handleMsgHeartbeatResponse(m pb.Message) {
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		} else {
			r.sendAppend(m.From) // send missing log
		}
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	for _, e := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: e.EntryType,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      e.Data,
		})
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	if len(r.peers) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	if !m.Reject {
		currentTerm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}
		if currentTerm == r.Term { // commit the current term TODO
			if _, ok := r.Prs[m.From]; !ok {
				r.Prs[m.From] = &Progress{Match: m.Index, Next: m.Index + 1}
			} else {
				r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
				r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
			}
			cnt := 1
			newCommitted := uint64(0)
			for peer, p := range r.Prs {
				if peer == r.id {
					continue
				}
				if p.Match > r.RaftLog.committed {
					cnt++
					if newCommitted == 0 {
						newCommitted = p.Match
					} else {
						newCommitted = min(p.Match, newCommitted)
					}
				}
			}
			//fmt.Printf("r.id: %d, cnt: %d, commit: %d\n", r.id, cnt, newCommitted)
			if cnt > len(r.peers)/2 {
				r.RaftLog.committed = newCommitted
				//fmt.Printf("id: %d, commit: %d\n", r.id, r.RaftLog.committed)
				for _, peer := range r.peers {
					if peer == r.id {
						continue
					}
					r.msgs = append(r.msgs, pb.Message{ // just for update the commitIndex
						From:    r.id,
						To:      peer,
						Term:    r.Term,
						MsgType: pb.MessageType_MsgAppend,
						Commit:  r.RaftLog.committed,
						Reject:  true,
					})
				}

				if r.RaftLog.committed > r.RaftLog.LastIndex() {
					panic("r.RaftLog.committed > r.RaftLog.LastIndex()")
				}
			}
		}
	} else if !m.NeedSnapshot {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return
		}
		preIndex := r.RaftLog.findConflictIndex(m.Index, m.LogTerm)
		_, err := r.RaftLog.Term(preIndex)
		for err != nil {
			preIndex--
			_, err = r.RaftLog.Term(preIndex)
		}
		r.Prs[m.From].Match = preIndex
		r.Prs[m.From].Next = preIndex + 1
		r.sendAppend(m.From)
	} else {
		// generate snapshot todo
		//snapshot, err := r.RaftLog.storage.Snapshot()
		//if err != nil {
		//	return
		//}
		//r.msgs = append(r.msgs, pb.Message{
		//	From:     r.id,
		//	To:       m.From,
		//	Term:     r.Term,
		//	MsgType:  pb.MessageType_MsgSnapshot,
		//	Commit:   r.RaftLog.committed,
		//	Snapshot: &snapshot,
		//})
	}
}

func (r *Raft) handleMsgRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	if !m.Reject {
		r.votesNum++
		if r.votesNum > len(r.peers)/2 {
			r.becomeLeader()
		}
	} else {
		rejectNum := 0
		for _, ok := range r.votes {
			if !ok {
				rejectNum++
				if rejectNum > len(r.peers)/2 {
					r.becomeFollower(r.Term, None)
					break
				}
			}
		}
	}
}

func (r *Raft) handleMsgRequestVote(m pb.Message) {
	newMsg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    max(r.Term, m.Term),
		Reject:  false,
	}

	if m.Term < r.Term {
		newMsg.Reject = true
		r.msgs = append(r.msgs, newMsg)
		return
	}

	if (m.LogTerm > r.RaftLog.LastTerm()) || (m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex()) { // 拥有最新的日志
		if r.Term == m.Term && r.Vote != None && r.Vote != m.From {
			newMsg.Reject = true
		} else {
			r.becomeFollower(m.Term, None)
			r.Vote = m.From
		}
	} else {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
		newMsg.Reject = true
	}

	r.msgs = append(r.msgs, newMsg)
}

func (r *Raft) handleMsgHup() {
	r.becomeCandidate()
	if len(r.peers) == 1 {
		r.becomeLeader()
	} else {
		r.sendElection()
	}
}

func convertEntryArray(ents []*pb.Entry) []pb.Entry {
	newEntries := make([]pb.Entry, 0)
	for _, e := range ents {
		newEntries = append(newEntries, *e)
	}
	return newEntries
}

func convertReferenceEntryArray(ents []pb.Entry) []*pb.Entry {
	newEntries := make([]*pb.Entry, 0)
	for _, e := range ents {
		entry := e // must be copy
		newEntries = append(newEntries, &entry)
	}
	return newEntries
}

// handleAppendEntries handle AppendEntries RPC request todo
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// fmt.Printf("handleAppendEntries, id : %d, msg: %v, state: %d, r.log: %v\n", r.id, m, r.State, r.RaftLog)
	// defer fmt.Printf("after handleAppendEntries, id : %d, msg: %v, state: %d, r.log: %v\n", r.id, m, r.State, r.RaftLog)
	new_msg := pb.Message{
		MsgType:      pb.MessageType_MsgAppendResponse,
		From:         r.id,
		To:           m.From,
		Term:         max(r.Term, m.Term),
		Reject:       false,
		NeedSnapshot: false,
	}

	if m.Term < r.Term {
		new_msg.Reject = true
		r.msgs = append(r.msgs, new_msg)
		return
	}

	r.becomeFollower(m.Term, m.From)
	dummyIndex := r.RaftLog.FirstIndex() - 1
	lastIndex := r.RaftLog.LastIndex()

	if m.Index < dummyIndex { // need snapshot
		new_msg.NeedSnapshot = true
		new_msg.Reject = true
	} else if m.Index > lastIndex {
		new_msg.Reject = true
		new_msg.Index = lastIndex
		new_msg.LogTerm = r.RaftLog.LastTerm()
	} else {
		term, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}

		if term == m.LogTerm {
			//fmt.Printf("m.Index: %d\n", m.Index)
			preLastIndex := r.RaftLog.LastIndex()
			new_msg.Index = r.appendEntries(convertEntryArray(m.Entries)...)
			if r.RaftLog.LastIndex() < r.RaftLog.stabled {
				log.Panicf("preLastIndex: %d, r.RaftLog.LastIndex(): %d < r.RaftLog.stabled: %d\n", preLastIndex, r.RaftLog.LastIndex(), r.RaftLog.stabled)
			}
			if m.Reject || (m.Entries != nil && len(m.Entries) > 0) { // update commitIndex
				r.RaftLog.committed = min(max(m.Commit, r.RaftLog.committed), r.RaftLog.LastIndex())
			} else if m.Entries == nil || len(m.Entries) == 0 { // update commitIndex for match index
				r.RaftLog.committed = min(r.RaftLog.LastIndex(), max(r.RaftLog.committed, min(m.Commit, m.Index)))
			}
		} else {
			// reject find conflict index
			hintIndex := r.RaftLog.findConflictIndex(m.Index, m.LogTerm)
			hintTerm, err := r.RaftLog.Term(hintIndex)
			if err != nil {
				panic(err)
			}
			new_msg.Index = hintIndex
			new_msg.LogTerm = hintTerm
			new_msg.Reject = true
		}
	}
	r.msgs = append(r.msgs, new_msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term || m.Index >= r.RaftLog.LastIndex() {
		r.becomeFollower(m.Term, m.From)
	}
	//fmt.Printf("id: %d, r.RaftLog.committed: %d, r.RaftLog.LastIndex(): %d, m.Commit: %d\n", r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), m.Commit)

	newMessage := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    max(r.Term, m.Term),
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
		Reject:  false,
	}

	if r.Term > m.Term || m.Commit > r.RaftLog.LastIndex() || r.RaftLog.committed < m.Commit {
		newMessage.Reject = true
	} else {
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), max(r.RaftLog.committed, m.Commit))
	}
	r.msgs = append(r.msgs, newMessage)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	fmt.Printf("handleSnapshot\n")
	if m.Snapshot == nil {
		panic("snapshot is nil.")
	}

	if m.Term < r.Term || r.RaftLog.FirstIndex() > m.Snapshot.Metadata.Index {
		return
	}

	r.Lead = m.From
	r.Term = max(r.Term, m.Snapshot.Metadata.Term)
	if r.RaftLog.FirstIndex() <= m.Snapshot.Metadata.Index && m.Snapshot.Metadata.Index <= r.RaftLog.LastIndex() {
		if len(r.RaftLog.entries) > 0 {
			r.RaftLog.entries = r.RaftLog.entries[m.Snapshot.Metadata.Index-r.RaftLog.entries[0].Index+1:]
		}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.committed = max(r.RaftLog.committed, m.Snapshot.Metadata.Index)

	if m.Snapshot.Metadata.ConfState.Nodes != nil && len(m.Snapshot.Metadata.ConfState.Nodes) > 0 {
		r.peers = m.Snapshot.Metadata.ConfState.Nodes
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
