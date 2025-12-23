package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

func (r *Raft) InstallSnapshot(s *pb.Snapshot) bool {
	l := r.RaftLog
	l.pendingSnapshot = s
	l.applied = s.Metadata.Index
	l.stabled = s.Metadata.Index
	l.entries = nil
	l.snapshotIndex = s.Metadata.Index
	l.snapshotTerm = s.Metadata.Term
	l.pendingSnapshot = s
	r.Prs = make(map[uint64]*Progress)
	for _, id := range s.Metadata.ConfState.Nodes {
		match := r.RaftLog.snapshotIndex
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
