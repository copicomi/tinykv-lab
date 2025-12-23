package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) InstallSnapshot(s *pb.Snapshot) bool {
	if s == nil || s.Metadata == nil {
		log.Warning("empty snapshot")
		return false
	}
	l := r.RaftLog
	// If snapshot is outdated, reject it
	if s.Metadata.Index <= l.committed {
		return true
	}

	l.pendingSnapshot = s
	l.applied = s.Metadata.Index
	l.committed = max(l.committed, s.Metadata.Index)
	l.stabled = s.Metadata.Index
	l.entries = []pb.Entry{pb.Entry{
		Term:  s.Metadata.Term,
		Index: s.Metadata.Index,
	}}
	l.snapshotIndex = s.Metadata.Index
	l.snapshotTerm = s.Metadata.Term

	r.peers = s.Metadata.ConfState.Nodes
	r.Prs = make(map[uint64]*Progress, len(r.peers))
	for _, id := range s.Metadata.ConfState.Nodes {
		match := l.snapshotIndex
		if id == r.id {
			match = s.Metadata.Index
		}
		r.Prs[id] = &Progress{
			Match: match,
			Next:  s.Metadata.Index + 1,
		}
	}
	mDebug(r, "install snapshot index=%d", s.Metadata.Index)
	return true
}
