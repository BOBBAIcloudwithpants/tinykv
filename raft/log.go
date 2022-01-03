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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

	received map[uint64]map[uint64]bool
	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	rl := new(RaftLog)
	rl.storage = storage
	rl.entries = make([]pb.Entry, 0)
	rl.received = make(map[uint64]map[uint64]bool)
	rl.loadEntriesFromStorage()

	return rl
}

func (l *RaftLog) entryExisted(idx uint64, logTerm uint64) bool{
	//fmt.Printf("寻找：index: %d, term: %d\n", idx, logTerm)
	if idx == 0 && logTerm == 0 {
		return true
	}

	for _, e := range l.entries {

		//fmt.Printf("e index: %d, logTerm: %d\n", e.Index, e.Term)
		if e.Term == logTerm && e.Index == idx {

			return true
		}
	}
	return false
}

func (l *RaftLog) isLogNewer(index uint64, term uint64) bool {
	if len(l.entries) == 0 {
		return true
	}
	ct, err := l.Term(index)
	if err != nil {
		if err == IndexOutOfBounds {
			return false
		} else if err == IndexNotExisted {
			// index > lastIndex
			t, _ := l.Term(l.LastIndex())
			return term >= t
		}
	}
	if ct != term {
		return term > ct
	}

	return index >= l.LastIndex()

}

func (l *RaftLog) matchEntriesAndAppend(index uint64, term uint64, entries []*pb.Entry) {

	if len(entries) == 0 {
		for i := len(l.entries) - 1; i >= 0; i-- {
			if index == l.entries[i].Index && term == l.entries[i].Term{
				l.entries = l.entries[:i+1]
				break
			}
		}
		return
	}

	var i int
	idx := entries[0].Index
	for i = len(l.entries) - 1; i >= 0; i-- {
		if idx == l.entries[i].Index {
			break
		}
	}

	ents := make([]pb.Entry, 0, len(entries))
	for _, e := range entries {
		ents = append(ents, *e)
	}

	if i == -1 {
		// no match, just append
		l.entries = append(l.entries, ents...)
	} else {
		old_len := len(l.entries)
		for k, e := range ents {
			if i >= old_len {
				l.entries = append(l.entries, ents[k:]...)
				break
			}
			if e.Term != l.entries[i].Term {
				// cover
				l.stabled = l.entries[i].Index - 1
				l.entries = append(l.entries[:i], ents[k:]...)
				break
			}
			i++
		}
	}
	// cover the entry at pos i+1
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
	if len(l.entries) > 0 && l.entries[0].Data == nil && l.entries[0].Index == 0{
		// skip noop entry
		l.entries = l.entries[1:]
	}
	ents := l.entries
	i := 0
	for ;i < len(ents); i++ {
		if l.entries[i].Index > l.stabled {
			break
		}
	}

	return ents[i:]
}

func (l *RaftLog) loadEntriesFromStorage() {
	fi, _ := l.storage.FirstIndex()
	li, _ := l.storage.LastIndex()
	ents, _ := l.storage.Entries(fi, li+1)
	//fmt.Printf("loadEntriesFromStorage li: %d, fi: %d, ents: %+v\n", li, fi, ents)
	l.entries = ents
	l.stabled = li
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	for _, e := range l.unstableEntries() {
		if e.Index > l.applied && e.Index <= l.committed {
			ents = append(ents, e)
		}
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries) - 1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	if len(l.entries) == 0 {
		return 0, IndexOutOfBounds
	}
	fi, li := l.entries[0].Index, l.entries[len(l.entries) - 1].Index
	if fi > i {
		return fi, IndexOutOfBounds
	}

	if li < i {
		return li, IndexNotExisted
	}
	return l.entries[i - fi].Term, nil
}

func (l *RaftLog) GetEntriesByIndex(i uint64) []*pb.Entry {
	ents := make([]*pb.Entry, 0)
	if len(l.entries) == 0 {
		return ents
	}
	fi, li := l.entries[0].Index, l.entries[len(l.entries) - 1].Index
	entries := l.entries
	if i != 0{
		if i == li {
			entries = entries[:0]
		} else {
			entries = entries[i - fi + 1:]
		}
	}
	for _, e := range entries {
		ep := new(pb.Entry)
		*ep = e
		ents = append(ents, ep)
	}
	return ents
}

func (l *RaftLog) AppendApplicationEntries(entries []*pb.Entry, firstIndex uint64, term uint64) error {
	//var ents []pb.Entry
	idx := firstIndex
	if term == 0 {
		term = 1
	}
	for _, e := range entries {
		if !isEmptyEntry(e) {
			e.Term = term
			e.Index = idx
			e.EntryType = pb.EntryType_EntryNormal
			//ents = append(ents, *e)
			l.entries = append(l.entries, *e)
			l.received[idx] = map[uint64]bool{}
			idx++
		}
	}
	return nil

	//err := l.storage.Append(ents)
	//l.applied = idx - 1
	//l.committed, _ = l.storage.LastIndex()
	//if err == nil {
	//	// new entries become stable
	//	l.applied = l.committed
	//} else {
	//	l.applied
	//}
}

func (l *RaftLog) commitEntries(committed uint64) {
	l.committed = committed
}

func (l *RaftLog) applyEntries(a uint64) {
	l.applied = a
}

//func (l *RaftLog) Received(index uint64, id uint64) {
//	l.received[index][id] = true
//}
//
//func (l *RaftLog) MajorityReceived(index uint64, total uint64) bool {
//	return uint64(2 * len(l.received[index])) > total
//}
