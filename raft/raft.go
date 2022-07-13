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

	hardState, confState, err := c.Storage.InitialState() // confState todo
	if err != nil {
		panic(err)
	}

	raft.Vote = hardState.Vote
	raft.Term = hardState.Term
	raft.RaftLog.committed = hardState.Commit

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
	// TODO from snapshot
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

func (r *Raft) GetHardState() pb.HardState {
	hardState, _, err := r.RaftLog.storage.InitialState()
	if err != nil {
		panic(err)
	}
	return hardState
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}

	newEntries := make([]*pb.Entry, 0)
	for _, e := range r.RaftLog.entries[r.RaftLog.GetRealIndex(r.Prs[to].Next):] {
		newEntries = append(newEntries, &pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      e.Term,
			Index:     e.Index,
			Data:      r.RaftLog.GetDataByIndex(e.Index),
		})
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
		Entries: newEntries,
		Commit:  r.RaftLog.committed,
	})
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
	r.msgs = append(r.msgs, pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed%r.heartbeatTimeout == 0 {
			r.heartbeatElapsed = 0
			r.sendHeartBeat()
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed%r.electionTimeout == 0 {
			r.electionElapsed = 0
			r.becomeCandidate()
			r.sendElection()
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

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	})

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	//fmt.Printf("become Leader, id : %d\n", r.id)
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		//fmt.Printf("id : %d, r.Prs[id].Next: %d\n", id, r.Prs[id].Next)
		r.sendAppendEntries(id, r.Prs[id].Next)
	}
}

func (r *Raft) sendAppendEntries(to uint64, startIndex uint64) {
	newEntries := make([]*pb.Entry, 0)
	nxt := r.RaftLog.GetRealIndex(startIndex)
	nextEntries := r.RaftLog.entries[nxt:]
	for i, _ := range nextEntries {
		newEntries = append(newEntries, &nextEntries[i])
	}

	logTerm, err := r.RaftLog.Term(startIndex - 1)
	if err != nil {
		panic(err)
	}
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index:   startIndex - 1,
		LogTerm: logTerm,
		Entries: newEntries,
		Commit:  r.RaftLog.committed,
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//if m.To != r.id {
	//	return nil
	//}

	if r.Term < m.Term {
		//fmt.Printf("msg: %v, id: %d\n", m, r.id)
		r.becomeFollower(m.Term, None)
	}

	if r.State != StateLeader && m.MsgType == pb.MessageType_MsgHup {
		r.handleMsgHup()
	}

	if r.State != StateLeader && m.MsgType == pb.MessageType_MsgHeartbeat {
		r.handleHeartbeat(m)
	}

	if m.MsgType == pb.MessageType_MsgRequestVote {
		r.handleMsgRequestVote(m)
	}

	if m.MsgType == pb.MessageType_MsgAppend {
		r.handleAppendEntries(m)
	}

	switch r.State {
	case StateFollower:
		break
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			r.handleMsgRequestVoteResponse(m)
		}
	case StateLeader:
		if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
			r.handleMsgHeartbeatResponse(m)
		}

		if m.MsgType == pb.MessageType_MsgAppendResponse {
			r.handleMsgAppendResponse(m)
		}

		if m.MsgType == pb.MessageType_MsgPropose {
			r.handleMsgPropose(m)
		}

		if m.MsgType == pb.MessageType_MsgBeat {
			r.sendHeartBeat()
		}
	}
	r.Term = max(r.Term, m.Term)
	return nil
}

func (r *Raft) handleMsgHeartbeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(r.Term, None)
	} else {
		if m.Index != 0 {
			if r.RaftLog.GetRealIndex(m.Index)+1 < uint64(len(r.RaftLog.entries)) &&
				(r.RaftLog.entries[r.RaftLog.GetRealIndex(m.Index)].Term == m.LogTerm) {
				r.sendAppendEntries(m.From, m.Index+1)
			}
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
		r.sendAppendEntries(peer, r.Prs[peer].Next)
	}
}

// todo
func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	if !m.Reject {
		if r.RaftLog.entries[r.RaftLog.GetRealIndex(m.Index)].Term == r.Term { // commit the current term TODO
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
			if cnt > len(r.peers)/2 {
				r.RaftLog.committed = newCommitted
				for _, peer := range r.peers {
					if peer == r.id {
						continue
					}
					r.msgs = append(r.msgs, pb.Message{
						From:    r.id,
						To:      peer,
						Term:    r.Term,
						MsgType: pb.MessageType_MsgAppend,
						Commit:  r.RaftLog.committed,
					})
				}

				for r.RaftLog.committed > r.RaftLog.LastIndex() {
					r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
						Term:  r.Term,
						Index: r.RaftLog.LastIndex() + 1,
					})
				}
			}
		}
	} else {
		newEntries := make([]*pb.Entry, 0)
		for i, _ := range r.RaftLog.entries[r.RaftLog.GetRealIndex(m.Index+1):] {
			newEntries = append(newEntries, &r.RaftLog.entries[r.RaftLog.GetRealIndex(m.Index+1):][i])
		}
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgAppend,
			Index:   m.Index,
			LogTerm: r.RaftLog.entries[r.RaftLog.GetRealIndex(m.Index)].Term,
			Entries: newEntries,
			Commit:  r.RaftLog.committed,
		})
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
					fmt.Printf("handleMsgRequestVoteResponse id: %d, becomeFollower\n", r.id)
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

	if (len(r.RaftLog.entries) == 0 || (m.Index <= r.RaftLog.LastIndex() && m.LogTerm > r.RaftLog.entries[r.RaftLog.GetRealIndex(m.Index)].Term)) ||
		(m.LogTerm > r.RaftLog.entries[len(r.RaftLog.entries)-1].Term) ||
		(m.LogTerm == r.RaftLog.entries[len(r.RaftLog.entries)-1].Term && m.Index >= r.RaftLog.LastIndex()) {
		if r.Term == m.Term && r.Vote != None && r.Vote != m.From {
			newMsg.Reject = true
		} else {
			r.becomeFollower(m.Term, None)
			r.Vote = m.From
		}
	} else {
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

// handleAppendEntries handle AppendEntries RPC request todo
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// fmt.Printf("handleAppendEntries, id : %d, msg: %v, state: %d, r.log: %v\n", r.id, m, r.State, r.RaftLog)
	// defer fmt.Printf("after handleAppendEntries, id : %d, msg: %v, state: %d, r.log: %v\n", r.id, m, r.State, r.RaftLog)
	new_msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    max(r.Term, m.Term),
		Reject:  false,
	}

	if m.Term < r.Term {
		new_msg.Reject = true
		r.msgs = append(r.msgs, new_msg)
		return
	}

	if len(r.RaftLog.entries) == 0 || m.Index == 0 {
		if len(r.RaftLog.entries) == 0 && m.Index == 0 {
			for _, e := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
			new_msg.Index = r.RaftLog.LastIndex()
			new_msg.Term = r.Term
			new_msg.LogTerm = r.RaftLog.LastTerm()
		} else if len(r.RaftLog.entries) == 0 {
			new_msg.Index = 0
			new_msg.Term = r.Term
			new_msg.LogTerm = 0
			new_msg.Reject = true
		} else {
			for _, e := range m.Entries {
				if e.Index > r.RaftLog.LastIndex() {
					r.RaftLog.entries = append(r.RaftLog.entries, *e)
					new_msg.Index = r.RaftLog.LastIndex()
					new_msg.LogTerm = r.RaftLog.entries[r.RaftLog.GetRealIndex(r.RaftLog.LastIndex())].Term
				} else if r.RaftLog.entries[r.RaftLog.GetRealIndex(e.Index)].Term != e.Term {
					if r.RaftLog.GetRealIndex(e.Index) == 0 {
						r.RaftLog.stabled = 0
						r.RaftLog.entries = make([]pb.Entry, 0)
						for _, e := range m.Entries {
							r.RaftLog.entries = append(r.RaftLog.entries, *e)
						}
						new_msg.Index = r.RaftLog.LastIndex()
						new_msg.LogTerm = r.RaftLog.entries[r.RaftLog.GetRealIndex(r.RaftLog.LastIndex())].Term
						break
					} else {
						new_msg.Reject = true
						logIndex := r.RaftLog.GetRealIndex(e.Index)
						conflictTerm := r.RaftLog.entries[logIndex].Term
						new_msg.Index = r.RaftLog.entries[logIndex].Index
						for i := logIndex; i >= 0; i-- {
							if conflictTerm == r.RaftLog.entries[i].Term {
								if i == 0 {
									break
								}
								continue
							} else {
								new_msg.Index = r.RaftLog.entries[i+1].Index
								break
							}

						}
						new_msg.LogTerm = conflictTerm
					}
				}
			}
		}
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
	} else {
		preLogIndex := r.RaftLog.GetRealIndex(m.Index)
		if preLogIndex >= uint64(len(r.RaftLog.entries)) {
			new_msg.Reject = true
			new_msg.Index = r.RaftLog.LastIndex()
			new_msg.LogTerm = r.RaftLog.entries[r.RaftLog.GetRealIndex(r.RaftLog.LastIndex())].Term
		} else {
			if r.RaftLog.entries[preLogIndex].Term != m.LogTerm {
				new_msg.Reject = true
				if preLogIndex == 0 {
					r.RaftLog.stabled = 0
					r.RaftLog.entries = make([]pb.Entry, 0)
					r.RaftLog.committed = 0
					new_msg.Index = 0
					new_msg.LogTerm = 0
				} else {
					conflictTerm := r.RaftLog.entries[preLogIndex].Term
					new_msg.Index = r.RaftLog.entries[preLogIndex].Index
					for i := preLogIndex; i >= 0; i-- {
						if conflictTerm == r.RaftLog.entries[i].Term {
							if i == 0 {
								break
							}
							continue
						} else {
							new_msg.Index = r.RaftLog.entries[i].Index
							new_msg.LogTerm = r.RaftLog.entries[i].Term
							break
						}
					}
					if new_msg.LogTerm == conflictTerm {
						new_msg.Index = r.RaftLog.entries[0].Index
						new_msg.LogTerm = r.RaftLog.entries[0].Term
					}
				}
			} else {
				oldMatchEntries := r.RaftLog.entries[:preLogIndex+1]
				isTheSame := true
				for i, e := range m.Entries {
					nxtIndex := preLogIndex + 1 + uint64(i)
					if nxtIndex < uint64(len(r.RaftLog.entries)) {
						if r.RaftLog.entries[preLogIndex+1+uint64(i)].Index != e.Index ||
							r.RaftLog.entries[preLogIndex+1+uint64(i)].Term != e.Term {
							isTheSame = false
						}
					}
					oldMatchEntries = append(oldMatchEntries, *e)
				}
				if isTheSame {
					if preLogIndex+uint64(len(m.Entries))+1 < uint64(len(r.RaftLog.entries)) {
						for _, e := range r.RaftLog.entries[preLogIndex+uint64(len(m.Entries))+1:] {
							oldMatchEntries = append(oldMatchEntries, e)
						}
					}
				} else {
					r.RaftLog.stabled = r.RaftLog.entries[preLogIndex].Index
				}
				r.RaftLog.entries = oldMatchEntries
				r.RaftLog.committed = min(m.Index+uint64(len(m.Entries)), m.Commit)
				new_msg.Index = r.RaftLog.LastIndex()
				new_msg.LogTerm = r.RaftLog.entries[r.RaftLog.GetRealIndex(r.RaftLog.LastIndex())].Term
			}
		}
	}
	r.msgs = append(r.msgs, new_msg)
	r.becomeFollower(m.Term, m.From)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term || m.Index >= r.RaftLog.LastIndex() {
		r.becomeFollower(m.Term, m.From)
	}
	r.RaftLog.committed = max(r.RaftLog.committed, m.Commit)
	newMessage := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    max(r.Term, m.Term),
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
		Reject:  false,
	}

	if r.Term > m.Term {
		newMessage.Reject = true
	}

	r.msgs = append(r.msgs, newMessage)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
