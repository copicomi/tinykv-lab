package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Vote = r.id
	r.votes[r.id] = true
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	// TODO: 初始化 leader 的 log 进度，并进行广播
	// NOTE: Leader should propose a noop entry on its term
}

func (r *Raft) findAnotherLeader(m pb.Message) bool {
	if m.Term > r.Term {
		return true
	}
	if m.Term == r.Term && isFromLeaderMsg(m.MsgType) {
		return true
	}
	return false
}
