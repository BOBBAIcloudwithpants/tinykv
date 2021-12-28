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
	r.votes = make(map[uint64]bool)
	for _, p := range c.peers {
		r.votes[p] = false
	}


	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTimeout = c.ElectionTick

	rand.Seed(time.Now().UnixNano())
	r.randomWaitTime = produceRandomElectionTerm(0,r.electionTimeout)

	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.Lead = 0
	r.State = StateFollower
	r.Term = 1
	r.Vote = 0
	// Your Code Here (2A).
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
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
	if r.State == StateFollower {
		if r.electionElapsed >= r.electionTimeout + r.randomWaitTime{
			// timeout, transfer state into StateCandidate
			//r.Term++
			//fmt.Printf("Term变了tick: id: %d state: %s, %d -> %d\n", r.id, r.State, r.Term, r.Term+1)
			//r.becomeCandidate()
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				Term:    r.Term,
				From:    r.id,
			})
		}
	} else if r.State == StateCandidate {
		if winElection(r) {
			r.becomeLeader()
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				Term:    r.Term,
				From:    r.id,
			})
		} else if r.electionElapsed >= r.randomWaitTime + r.electionTimeout {
			// current election timeout, retry
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

	rand.Seed(time.Now().UnixNano())
	r.randomWaitTime = produceRandomElectionTerm(0,r.electionTimeout)

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
	//r.Term++
	// vote for myself
	for k, _ := range r.votes {
		r.votes[k] = false
	}
	r.Vote = r.id
	r.votes[r.id] = true

	rand.Seed(time.Now().UnixNano())
	r.randomWaitTime = produceRandomElectionTerm(0,r.electionTimeout)

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
				r.Term++
				if r.State != StateCandidate {
					r.becomeCandidate()
				}
				// candidate send RequestVote
				for k, _ := range r.votes {
					if k != r.id {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVote,
							From:    r.id,
							To:      k,
							Term:    r.Term,
						})
					}
				}

				if winElection(r) {
					r.becomeLeader()
					r.Step(pb.Message{
						MsgType: pb.MessageType_MsgBeat,
						Term:    r.Term,
						From:    r.id,
					})
				}
			}
		} else if m.MsgType == pb.MessageType_MsgBeat {
			// leader send heartbeats
			if r.State == StateLeader && r.id == m.From{
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
			}
		}
	} else {
		// not local message

		if m.Term >= r.Term && IsFromLeader(m.MsgType) {
			// update Term and role based on current role
			r.becomeFollower(m.Term, m.From)
			if m.MsgType == pb.MessageType_MsgHeartbeat {
				r.handleHeartbeat(m)
			}
		} else {
			switch r.State {
			case StateFollower:
				if m.To == r.id {
					if m.MsgType == pb.MessageType_MsgHeartbeat {
						r.handleHeartbeat(m)
					} else if m.MsgType == pb.MessageType_MsgRequestVote {
						// vote for candidate
						rej := false
						if r.Vote != 0 && r.Vote != m.From {
							rej = true
						}

						if !rej {
							//fmt.Printf("MessageType_MsgRequestVote,从%d到%d, Term变了，id: %d, state: %s, %d -> %d\n",m.From,r.id, r.id, r.State, r.Term, m.Term)
							r.Vote = m.From
							r.Term = m.Term
						}
						r.electionElapsed = 0

						r.msgs = append(r.msgs, pb.Message{
							From:    r.id,
							To:      m.From,
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							Reject:  rej,
							Term:    r.Term,
						})
					}
				}
			case StateCandidate:
				if m.To == r.id {
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
						if m.Reject == false {
							r.votes[m.From] = true
							if winElection(r) {
								r.becomeLeader()
								r.Step(pb.Message{
									MsgType: pb.MessageType_MsgBeat,
									Term:    r.Term,
									From:    r.id,
								})
							}
						}
					} else if m.MsgType == pb.MessageType_MsgHeartbeat {
						if m.Term >= r.Term {
							// become follower
							r.becomeFollower(m.Term, m.From)
							r.handleHeartbeat(m)
						} else {
							r.msgs = append(r.msgs, pb.Message{
								Term: r.Term,
								From: r.id,
								To: m.From,
								MsgType: pb.MessageType_MsgAppendResponse,
							})
						}
					} else if m.MsgType == pb.MessageType_MsgAppend {
						// become follower
						r.becomeFollower(m.Term, m.From)

					}
				}
			case StateLeader:
				if m.To == r.id {
					if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
						r.electionElapsed = 0
					} else if m.MsgType == pb.MessageType_MsgRequestVote {
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
						}
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
						}
					}
				}
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    m.Term,
	})
	r.electionElapsed = 0
	//fmt.Printf("handleHeartbeat,从%d到%d: Term变了，id: %d, state: %s, %d -> %d\n",m.From, r.id, r.id, r.State, r.Term, m.Term)
	r.Term = m.Term
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
