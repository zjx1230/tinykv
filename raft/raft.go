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

	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range raft.peers {
		raft.Prs[peer] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}

		if peer == raft.id {
			raft.Prs[peer].Match = lastIndex
		}
	}

	//if _, ok := raft.Prs[raft.id]; !ok {
	//	raft.Prs[raft.id] = &Progress{
	//		Match: lastIndex,
	//		Next:  lastIndex + 1,
	//	}
	//}
	raft.electionTimeout = raft.electionTick + rand.Intn(10000)%c.ElectionTick
	raft.RaftLog.applied = max(raft.RaftLog.applied, c.Applied)
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
	if _, ok := r.Prs[r.id]; ok {
		r.Prs[r.id].Next = max(r.Prs[r.id].Next, lastIndex+1)
		r.Prs[r.id].Match = r.Prs[r.id].Next - 1
	}
	return lastIndex
}

func (r *Raft) sendSnapshot(to uint64) {
	if r.State != StateLeader {
		return
	}

	//var snapshot *pb.Snapshot
	//if r.RaftLog.pendingSnapshot != nil {
	//	snapshot = r.RaftLog.pendingSnapshot
	//} else {
	//
	//}
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			return
		}
		log.Panicf("Snapshot err: %v\n", err)
		return
	}
	//snapshot = &snap

	r.msgs = append(r.msgs, pb.Message{
		From:     r.id,
		To:       to,
		Term:     r.Term,
		MsgType:  pb.MessageType_MsgSnapshot,
		Snapshot: &snapshot,
	})
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}

	if r.RaftLog.pendingSnapshot != nil && r.Prs[to].Next <= r.RaftLog.pendingSnapshot.Metadata.Index {
		r.sendSnapshot(to)
		return true
	}

	if r.Prs[to].Next < r.RaftLog.FirstIndex() {
		r.sendSnapshot(to)
		return true
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
	//fmt.Printf("to: %d, newEntries: %v\n r.msg: %v\n", to, newEntries, r.msgs[len(r.msgs)-1])
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

func (r *Raft) sendMsgTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
		//Commit:  r.RaftLog.committed,
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

	r.appendEntries(pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
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
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
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
	case pb.MessageType_MsgTimeoutNow:
		r.handleMsgTimeOutNow(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
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
	case pb.MessageType_MsgTimeoutNow:
		r.handleMsgTimeOutNow(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
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
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
}

func (r *Raft) handleMsgTimeOutNow(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok { // has been removed
		return
	}

	r.becomeFollower(max(r.Term, m.Term), None)
	r.handleMsgHup()
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	isPeerExist := false
	for _, peerId := range r.peers {
		if peerId == m.From {
			isPeerExist = true
			break
		}
	}

	if !isPeerExist {
		return
	}

	if r.leadTransferee != None && m.From == r.id {
		r.becomeFollower(max(r.Term, m.Term), None)
		r.handleMsgHup()
		return
	}

	if r.State != StateLeader {
		if r.Lead != None {
			r.msgs = append(r.msgs, pb.Message{
				From:    m.From,
				To:      r.Lead,
				MsgType: pb.MessageType_MsgTransferLeader,
			})
		}
		return
	}

	if m.From == r.id {
		return
	}

	if r.Prs[m.From].Next > r.RaftLog.LastIndex()+1 {
		panic(fmt.Sprintf("r.Prs[m.From].Next: %d > r.RaftLog.LastIndex()+1: %d\n", r.Prs[m.From].Next, r.RaftLog.LastIndex()+1))
	}

	r.leadTransferee = m.From

	if r.Prs[m.From].Next == r.RaftLog.LastIndex()+1 { // 最新
		r.sendMsgTimeoutNow(m.From)
		r.leadTransferee = None
		//r.becomeFollower(max(r.Term, m.Term), None)
	} else {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleMsgHeartbeatResponse(m pb.Message) {
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
	} else {
		r.sendAppend(m.From) // send missing log
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	if r.leadTransferee != None { // reject the propose message
		return
	}

	if r.State != StateLeader {
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
		return
	}

	for _, e := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: e.EntryType,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      e.Data,
		})
		if r.PendingConfIndex == None && e.EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = r.RaftLog.LastIndex()
		}
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

func (r *Raft) mayBeCommit() {
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

	if len(r.peers) == 1 {
		newCommitted = max(r.Prs[r.peers[0]].Match, r.RaftLog.committed)
	}

	//fmt.Printf("r.id: %d, cnt: %d, commit: %d, len(r.peers): %d\n", r.id, cnt, newCommitted, len(r.peers))
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
				Index:   r.Prs[peer].Next - 1,
				Commit:  r.RaftLog.committed,
				Reject:  true,
			})
		}

		if r.RaftLog.committed > r.RaftLog.LastIndex() {
			panic("r.RaftLog.committed > r.RaftLog.LastIndex()")
		}
	}
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	//fmt.Printf("handleMsgAppendResponse, m: %s\n", m.String())
	if !m.Reject {
		if r.leadTransferee != None && m.Index == r.RaftLog.LastIndex() {
			r.leadTransferee = None
			r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
			r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
			r.sendMsgTimeoutNow(m.From)
			r.becomeFollower(max(m.Term, r.Term), None)
			return
		}

		currentTerm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}
		//
		//fmt.Printf("m.Index: %d, currentTerm: %d, m.logTerm: %d\n", m.Index, currentTerm, m.LogTerm)
		if currentTerm == r.Term { // commit the current term TODO
			if _, ok := r.Prs[m.From]; !ok {
				r.Prs[m.From] = &Progress{Match: m.Index, Next: m.Index + 1}
			} else {
				r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
				r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
			}
			//fmt.Printf("from: %d, match: %d\n", m.From, r.Prs[m.From].Match)
			r.mayBeCommit()
		}

		//if r.RaftLog.LastIndex() > m.Index {
		//	r.sendAppend(m.From)
		//}
	} else if !m.NeedSnapshot && m.Index >= r.RaftLog.FirstIndex()-1 {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return
		}
		//fmt.Printf("m.Index: %d, m.LogTerm: %d, r.RaftLog.LastIndex(): %d\n", m.Index, m.LogTerm, r.RaftLog.LastIndex())
		preIndex := r.RaftLog.findConflictIndex(m.Index, m.LogTerm)
		//fmt.Printf("preIndex: %d\n", preIndex)
		//if preIndex < r.RaftLog.dummyIndex || preIndex == 0 {
		//	//fmt.Printf("m.Index: %d, preIndex: %d, r.RaftLog.dummyIndex: %d\n", m.Index, preIndex, r.RaftLog.dummyIndex)
		//	r.sendSnapshot(m.From)
		//	return
		//}
		_, err := r.RaftLog.Term(preIndex)
		for err != nil {
			//fmt.Printf("err: %v\n", err)
			//if preIndex < r.RaftLog.dummyIndex || preIndex == 0 {
			//	r.sendSnapshot(m.From)
			//	return
			//}
			preIndex--
			_, err = r.RaftLog.Term(preIndex)
		}
		r.Prs[m.From].Match = preIndex
		r.Prs[m.From].Next = preIndex + 1
		r.sendAppend(m.From)
	} else {
		// generate snapshot
		//fmt.Printf("generate snapshot, msg: %v, next: %d\n", m, r.Prs[m.From].Next)
		r.sendSnapshot(m.From)
	}
}

func (r *Raft) handleMsgRequestVoteResponse(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok { // has been removed
		return
	}

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

	if m.Reject { // just update the commitIndex
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
		}
		return
	}

	if m.Index < dummyIndex { // need snapshot
		//fmt.Printf("need snapshot: m.Index: %d, dummyIndex: %d\n", m.Index, dummyIndex)
		//panic("m.Index < dummyIndex")
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

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	newMessage := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}

	r.msgs = append(r.msgs, newMessage)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}

	//if m.Term >= r.Term || m.Index >= r.RaftLog.LastIndex() {
	//	r.becomeFollower(m.Term, m.From)
	//}
	//fmt.Printf("id: %d, r.RaftLog.committed: %d, r.RaftLog.LastIndex(): %d, m.Commit: %d\n", r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), m.Commit)
	r.becomeFollower(m.Term, m.From)
	r.sendHeartbeatResponse(m.From, false)
	//newMessage := pb.Message{
	//	MsgType: pb.MessageType_MsgHeartbeatResponse,
	//	From:    r.id,
	//	To:      m.From,
	//	Term:    max(r.Term, m.Term),
	//	Index:   r.RaftLog.LastIndex(),
	//	LogTerm: r.RaftLog.LastTerm(),
	//	Reject:  false,
	//}
	//
	//if r.Term > m.Term || m.Commit > r.RaftLog.LastIndex() || r.RaftLog.committed < m.Commit {
	//	newMessage.Reject = true
	//} else {
	//	r.RaftLog.committed = min(r.RaftLog.LastIndex(), max(r.RaftLog.committed, m.Commit))
	//}
	//r.msgs = append(r.msgs, newMessage)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Snapshot == nil {
		panic("snapshot is nil.")
	}

	if m.Term < r.Term || r.RaftLog.committed >= m.Snapshot.Metadata.Index {
		term, err := r.RaftLog.Term(r.RaftLog.committed)
		if err != nil {
			panic(err)
		}
		//fmt.Printf("commit term: %d\n", term)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    m.To,
			Index:   r.RaftLog.committed,
			Term:    r.Term,
			LogTerm: term,
			Commit:  r.RaftLog.committed,
			Reject:  true,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)
	term, err := r.RaftLog.Term(m.Snapshot.Metadata.Index)
	if err != nil || term != m.Snapshot.Metadata.Term { // restore
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.RaftLog.dummyIndex = m.Snapshot.Metadata.Index
		r.RaftLog.dummyTerm = m.Snapshot.Metadata.Term
		//fmt.Printf("handleSnapshot, id: %d, Snapshot.Metadata.Index: %d, m.Snapshot.Metadata.Term: %d\n", r.id, m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term)
	}
	r.RaftLog.entries = make([]pb.Entry, 0)
	r.RaftLog.committed = max(r.RaftLog.committed, m.Snapshot.Metadata.Index)
	r.RaftLog.applied = max(r.RaftLog.applied, m.Snapshot.Metadata.Index)
	r.RaftLog.stabled = max(r.RaftLog.applied, m.Snapshot.Metadata.Index)
	if m.Snapshot.Metadata.ConfState.Nodes != nil && len(m.Snapshot.Metadata.ConfState.Nodes) > 0 {
		r.peers = m.Snapshot.Metadata.ConfState.Nodes
	}

	for i := range r.peers {
		if _, ok := r.Prs[r.peers[i]]; ok {
			r.Prs[r.peers[i]].Match = r.RaftLog.LastIndex()
			r.Prs[r.peers[i]].Next = r.RaftLog.LastIndex() + 1
		} else {
			r.Prs[r.peers[i]] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    m.To,
		Index:   r.RaftLog.LastIndex(),
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Commit:  r.RaftLog.committed,
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	for i := range r.peers {
		if r.peers[i] == id {
			return
		}
	}
	r.peers = append(r.peers, id)
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	isContained := false
	index := -1
	for i := range r.peers {
		if r.peers[i] == id {
			index = i
			isContained = true
			break
		}
	}

	if !isContained {
		return
	}

	delete(r.Prs, id)
	newPeers := make([]uint64, 0)
	newPeers = append(newPeers, r.peers[:index]...)
	newPeers = append(newPeers, r.peers[index+1:]...)
	r.peers = newPeers
	r.PendingConfIndex = None

	if r.State == StateLeader {
		r.mayBeCommit()
	}
}
