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
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
	offset uint64
	// last index of snapshot
	snapshotIndex uint64
	// term
	snapshotTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	first_index, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	last_index, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(first_index, last_index+1)
	if err != nil {
		panic(err)
	}
	snapshotTerm, err := storage.Term(first_index - 1)
	if err != nil {
		panic(err)
	}
	l := &RaftLog{
		storage:       storage,
		applied:       first_index - 1,
		committed:     first_index - 1,
		stabled:       last_index,
		entries:       entries,
		offset:        first_index,
		snapshotIndex: first_index - 1,
		snapshotTerm:  snapshotTerm,
	}
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.entries[l.stabled+1-l.offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.entries[l.applied+1-l.offset : l.committed+1-l.offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.offset + uint64(len(l.entries)) - 1
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	return l.snapshotIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == l.snapshotIndex {
		return l.snapshotTerm, nil
	}
	if i < l.offset {
		return 0, ErrCompacted
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.offset].Term, nil
}

func (l *RaftLog) append(entry pb.Entry) {
	l.entries = append(l.entries, entry)
}

func (l *RaftLog) appendEntries(entries []*pb.Entry, prev_index uint64) {
	for i, entry := range entries {
		index := prev_index + uint64(i) + 1
		term, _ := l.Term(index)
		if index <= l.LastIndex() {
			if term != entry.Term {
				l.entries = l.entries[:index-l.offset]
				l.stabled = min(l.stabled, index-1)
			} else {
				continue
			}
		}
		l.append(*entry)
	}
}

func (l *RaftLog) nextEntries(next uint64) ([]*pb.Entry, error) {
	entries := make([]*pb.Entry, 0)
	if next <= l.snapshotIndex { // offset - 1
		return nil, ErrCompacted
	}
	for _, entry := range l.entries[next-l.offset:] {
		entries = append(entries, &pb.Entry{
			Term:  entry.Term,
			Index: entry.Index,
			Data:  entry.Data,
		})
	}
	return entries, nil
}
