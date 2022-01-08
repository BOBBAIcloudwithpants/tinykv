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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
	"math/rand"
	"time"
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

type Raft struct {
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

	// current vote number
	VoteNum uint64

	// has vote
	VoteTerm uint64

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

	randomWaitTime int

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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	r := new(Raft)
	r.id = c.ID

	// init votes
	r.votes = make(map[uint64]bool)
	for _, p := range c.peers {
		r.votes[p] = false
	}

	// init Progress
	r.Prs = make(map[uint64]*Progress)
	for _, p := range c.peers {
		r.Prs[p] = new(Progress)
		r.Prs[p].Match, r.Prs[p].Next = 0, 1
	}

	//fmt.Println("初始化之后：")
	//for k, p := range r.Prs {
	//	fmt.Printf("id: %d, match: %d, next: %d\n", k, p.Match, p.Next)
	//}

	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTimeout = c.ElectionTick

	rand.Seed(time.Now().UnixNano())
	r.randomWaitTime = produceRandomElectionTerm(0, r.electionTimeout)

	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.Lead = 0
	r.State = StateFollower
	r.Term = 0
	r.Vote = 0
	r.VoteNum = 0
	r.RaftLog = newLog(c.Storage)
	r.received(r.id, r.RaftLog.LastIndex())
	hs, _, _ := r.RaftLog.storage.InitialState()
	r.setHardState(hs)

	return r
}

func (r *Raft) setHardState(hs pb.HardState) {
	if hs.Term != 0 {
		r.Term = hs.Term
	}
	if hs.Vote != 0 {
		r.Vote = hs.Vote
		r.VoteTerm = hs.Term
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if len(r.RaftLog.entries) == 0 {
		// append noop entry
		r.RaftLog.AppendApplicationEntries([]*pb.Entry{new(pb.Entry)}, r.RaftLog.committed, r.Term)
	}
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		From:    r.id,
		To:      to,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	r.electionElapsed++
	if r.Term == 0 {
		r.Term = 1
	}
	if r.State == StateFollower {
		if r.electionElapsed >= r.electionTimeout+r.randomWaitTime {
			// timeout, transfer state into StateCandidate
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				Term:    r.Term,
				From:    r.id,
			})
		}
	} else if r.State == StateCandidate {
		if winElection(r) {
			r.becomeLeader()
			r.appendEmptyLogAfterWinElection()
		} else if r.electionElapsed > r.randomWaitTime+r.electionTimeout {
			// current election timeout, retry
			//fmt.Printf("过期了：%d %d\n", r.electionTimeout + r.randomWaitTime, r.electionTimeout)
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				Term:    r.Term,
				From:    r.id,
			})
		}

	} else if r.State == StateLeader {
		//if r.electionElapsed >= r.electionTimeout {
		//	// current election timeout, retry
		//	r.becomeCandidate()
		//}
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}
	// Your Code Here (2A).
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	r.electionElapsed = 0
	r.VoteNum = 0
	rand.Seed(time.Now().UnixNano())
	r.randomWaitTime = produceRandomElectionTerm(0, r.electionTimeout)

	for k, _ := range r.votes {
		r.votes[k] = false
	}

	// clear message
	r.msgs = make([]pb.Message, 0)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.electionElapsed = 0
	r.Term++
	r.VoteTerm = r.Term
	// vote for myself
	for k, _ := range r.votes {
		r.votes[k] = false
	}
	r.Vote = r.id
	r.votes[r.id] = true
	r.VoteNum = 1

	rand.Seed(time.Now().UnixNano())
	r.randomWaitTime = produceRandomElectionTerm(0, r.electionTimeout)

	// clear message
	r.msgs = make([]pb.Message, 0)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.VoteNum = 0
	r.VoteTerm = r.Term

	for k, _ := range r.votes {
		r.votes[k] = false
	}

	// clear message
	r.msgs = make([]pb.Message, 0)

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//printMsg(&m)
	if IsLocalMsg(m.MsgType) {
		// local message: MsgHup or MsgBeat
		if m.MsgType == pb.MessageType_MsgHup {
			if r.State == StateLeader {
				// just send Heartbeat
				r.heartbeatElapsed = 0
				for k, _ := range r.votes {
					if k != r.id {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgHeartbeat,
							From:    r.id,
							To:      k,
							Term:    r.Term,
						})
					}
				}
			} else {
				//fmt.Printf("#%d 开始选举\n", r.id)
				//r.Term++
				r.becomeCandidate()
				// candidate send RequestVote
				li := r.RaftLog.LastIndex()
				lt, _ := r.RaftLog.Term(li)
				for k, _ := range r.votes {
					if k != r.id {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVote,
							From:    r.id,
							To:      k,
							Term:    r.Term,
							Index:   li,
							LogTerm: lt,
						})
					}
				}

				if winElection(r) {
					r.becomeLeader()
					r.appendEmptyLogAfterWinElection()
				}
			}
		} else if m.MsgType == pb.MessageType_MsgBeat {
			// leader send heartbeats
			if r.State == StateLeader && r.id == m.From {
				//li := r.RaftLog.LastIndex()
				//lt, _ := r.RaftLog.Term(li)
				for k, _ := range r.votes {
					if k != r.id {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgHeartbeat,
							From:    r.id,
							To:      k,
							Term:    r.Term,
							Commit:  r.RaftLog.committed,
						})
					}
				}
			}
		} else if m.MsgType == pb.MessageType_MsgPropose {
			if r.State == StateLeader {
				ents := m.Entries
				if ents == nil {
					// inform others
					for k, _ := range r.Prs {
						if k != r.id {
							r.informPeerCommitment(k)
						}
					}
				} else {
					li := r.RaftLog.LastIndex()
					err := r.RaftLog.AppendApplicationEntries(ents, li+1, r.Term)
					if err != nil {
						log.Fatal(err.Error())
						return err
					}
					r.received(r.id, r.RaftLog.LastIndex())
					if len(r.Prs) == 1 {
						r.handleCommit(r.RaftLog.LastIndex())
					}
					for k, _ := range r.Prs {
						if k != r.id {
							r.syncWithPeers(k)
						}
					}

				}
			}
		}
	} else {
		// not local message

		if m.Term >= r.Term && IsFromLeader(m) {
			// update Term and role based on current role
			r.becomeFollower(m.Term, m.From)
			if m.MsgType == pb.MessageType_MsgHeartbeat {
				r.handleHeartbeat(m)
			}
		} else {
			switch r.State {
			case StateFollower:
				if m.MsgType == pb.MessageType_MsgHeartbeat {
					r.handleHeartbeat(m)
				} else if m.MsgType == pb.MessageType_MsgRequestVote {
					// vote for candidate
					rej := false
					if r.Vote != 0 && r.VoteTerm == 0 {
						r.VoteTerm = r.Term
					}

					if m.Term < r.Term || (r.Vote != 0 && r.Vote != m.From && r.VoteTerm == m.Term) || !r.RaftLog.isLogNewer(m.Index, m.LogTerm) {
						rej = true
					}

					// update term if needed
					if m.Term > r.Term {
						r.becomeFollower(m.Term, None)
					}
					if !rej {
						r.Vote = m.From
						r.Term = m.Term
						r.VoteTerm = m.Term
					}
					r.electionElapsed = 0
					r.msgs = append(r.msgs, pb.Message{
						From:    r.id,
						To:      m.From,
						MsgType: pb.MessageType_MsgRequestVoteResponse,
						Reject:  rej,
						Term:    r.Term,
					})
				} else if m.MsgType == pb.MessageType_MsgAppend {
					r.becomeFollower(m.Term, m.From)
					r.handleAppendEntries(m)
				}
			case StateCandidate:
				if m.MsgType == pb.MessageType_MsgRequestVote {
					if m.Term > r.Term {
						// become other's follower & vote for others
						r.becomeFollower(m.Term, m.From)
						r.Vote = m.From
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							To:      m.From,
							Reject:  false,
							Term:    m.Term,
						})

					} else {
						// reject other's vote
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							To:      m.From,
							Reject:  true,
							Term:    r.Term,
						})
					}

				} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
					// check other's vote
					r.VoteNum++
					//fmt.Printf("#%d: %t\n", m.From, m.Reject)
					if m.Reject == false {
						r.votes[m.From] = true
						if winElection(r) {
							//fmt.Printf("wins: %d\n", r.id)
							r.becomeLeader()
							// insert an empty entry
							r.appendEmptyLogAfterWinElection()
						}
					}

					if int(r.VoteNum) == len(r.votes) && !winElection(r) {
						//fmt.Printf("failed: %d\n", r.id)
						// election has end, the candidate lose
						r.becomeFollower(r.Term, m.From)
					}

				} else if m.MsgType == pb.MessageType_MsgHeartbeat {
					if m.Term >= r.Term {
						// become follower
						r.becomeFollower(m.Term, m.From)
						r.handleHeartbeat(m)
					} else {
						r.msgs = append(r.msgs, pb.Message{
							Term:    r.Term,
							From:    r.id,
							To:      m.From,
							MsgType: pb.MessageType_MsgAppendResponse,
						})
					}
				} else if m.MsgType == pb.MessageType_MsgAppend {
					// become follower
					if m.Term >= r.Term {
						r.becomeFollower(m.Term, m.From)
						r.handleAppendEntries(m)
					}
				}
			case StateLeader:
				if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
					r.electionElapsed = 0

					// set peers progress
					r.received(m.From, m.Index)

				} else if m.MsgType == pb.MessageType_MsgRequestVote {
					reject := false
					if r.Term >= m.Term || !r.RaftLog.isLogNewer(m.Index, m.LogTerm) {
						reject = true
					}
					// become other's follower & vote for others
					if m.Term > r.Term {
						r.becomeFollower(m.Term, None)
					}
					if !reject {
						r.Vote = m.From
						r.VoteTerm = m.Term
					}
					r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgRequestVoteResponse,
						From:    r.id,
						To:      m.From,
						Reject:  reject,
						Term:    m.Term,
					})
				} else if m.MsgType == pb.MessageType_MsgAppendResponse {
					if m.Term > r.Term {
						r.becomeFollower(m.Term, m.From)
						r.Vote = m.From
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							From:    r.id,
							To:      m.From,
							Reject:  false,
							Term:    m.Term,
						})
					} else {
						// check whether the data is received by majority of nodes
						if !m.Reject {
							r.received(m.From, m.Index)
							commitChange := false
							maxCommitted := uint64(0)

							for i := r.RaftLog.committed + 1; i <= m.Index; i++ {
								if r.majorityReceived(i) {
									maxCommitted = max(maxCommitted, i)
									commitChange = true
								}
							}
							// inform followers that the commit is updated
							if commitChange {
								r.handleCommit(maxCommitted)
								r.informCommitment()
							}

						} else {
							// retry
							r.received(m.From, m.Index-1)
							r.syncWithPeers(m.From)
						}
					}
				} else if m.MsgType == pb.MessageType_MsgAppend {
					// become follower
					if m.Term > r.Term {
						r.becomeFollower(m.Term, m.From)
					}
				}
			}
		}
	}
	return nil
}

func (r *Raft) informCommitment() {
	r.Step(pb.Message{
		Term:    r.Term,
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		Entries: nil,
	})
}

func (r *Raft) informPeerCommitment(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		//Entries: []*pb.Entry{{Data: nil}},
		Commit: r.RaftLog.committed,
	})
}

func (r *Raft) syncWithPeers(to uint64) {

	i, t := r.findMatchedIndexAndTerm(to)
	r.msgs = append(r.msgs, pb.Message{
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Index:   i,
		Entries: r.RaftLog.GetEntriesByIndex(i),
		LogTerm: t,
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) findMatchedIndexAndTerm(to uint64) (uint64, uint64) {
	fi := r.Prs[to].Match
	ft, err := r.RaftLog.Term(fi)
	if err != nil {
		if err == IndexOutOfBounds {
			return 0, r.Term
		} else if err == IndexNotExisted {
			fi = ft
			ft, _ = r.RaftLog.Term(fi)
			return fi, ft
		}
	}

	r.Prs[to].Match = fi
	return fi, ft
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		reject := false
		if !r.RaftLog.entryExisted(m.Index, m.LogTerm) {
			// reject
			reject = true
		}
		if !reject {
			r.RaftLog.matchEntriesAndAppend(m.Index, m.LogTerm, m.Entries)

			if m.Commit > r.RaftLog.committed {
				r.RaftLog.commitEntries(min(r.RaftLog.LastIndex(), m.Commit))
			}
			r.Term = m.Term
		}
		fi, ft := m.Index, m.LogTerm
		if !reject {
			fi = r.RaftLog.LastIndex()
			ft, _ = r.RaftLog.Term(fi)
		}
		r.msgs = append(r.msgs, pb.Message{
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  reject,
			Index:   fi,
			LogTerm: ft,
		})

	case StateCandidate:
	case StateLeader:
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	li := r.RaftLog.LastIndex()
	lt, _ := r.RaftLog.Term(li)
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    m.Term,
		Index:   li,
		LogTerm: lt,
		Commit:  r.RaftLog.committed,
	})
	r.electionElapsed = 0
	//fmt.Printf("handleHeartbeat,从%d到%d: Term变了，id: %d, state: %s, %d -> %d\n",m.From, r.id, r.id, r.State, r.Term, m.Term)
	r.Term = m.Term
	r.handleCommit(m.Commit)
}

func (r *Raft) received(id uint64, match uint64) {
	r.Prs[id].Match = match
	r.Prs[id].Next = match + 1
}

func (r *Raft) appendEmptyLogAfterWinElection() {
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Term:    r.Term,
		From:    r.id,
		Entries: []*pb.Entry{{Term: r.Term}},
		LogTerm: r.Term,
	})
}

func (r *Raft) majorityReceived(idx uint64) bool {
	recv := 0
	t, err := r.RaftLog.Term(idx)
	isInTerm := err != nil || t == r.Term
	for _, v := range r.Prs {
		//fmt.Printf("PEER: %d，Match：%d\n",  k, v.Match)
		if idx <= v.Match {
			recv++
		}
	}
	return isInTerm && 2*recv >= len(r.Prs)
}

func (r *Raft) handleCommit(c uint64) {
	if c > r.RaftLog.committed {
		// TODO: if committed is increased, apply to local state machine
	}
	r.received(r.id, c)
	r.RaftLog.committed = c
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
