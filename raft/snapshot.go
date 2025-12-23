package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

func (r *Raft) InstallSnapshot(s *pb.Snapshot) bool {
	r.RaftLog.pendingSnapshot = s
	r.RaftLog.applied = s.Metadata.Index
	r.RaftLog.offset = s.Metadata.Index
	r.RaftLog.stabled = s.Metadata.Index
	r.RaftLog.entries = nil
	r.Prs = make(map[uint64]*Progress)
	r.Prs[s.Metadata.ConfState.Nodes[0]] = &Progress{
		Match: s.Metadata.Index,
		Next:  s.Metadata.Index + 1,
	}
	mDebug(r, "install snapshot index=%d", s.Metadata.Index)
	return true
}
