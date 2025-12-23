package raft

import "github.com/pingcap-incubator/tinykv/log"

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Lead = lead
	if term > r.Term {
		r.Vote = None
	}
	r.Term = term
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Vote = r.id
	for _, peer := range r.peers {
		r.votes[peer] = false
	}
	r.votes[r.id] = true
	r.rejects_count = 0
	r.Term++
	// log.Warningf("[%d] S%d becomeCandidate", r.Term, r.id)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	log.Warningf("[%d] S%d becomeLeader", r.Term, r.id)
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = r.id
	lastIndex := r.RaftLog.LastIndex()
	for _, id := range r.peers {
		r.Prs[id] = &Progress{
			Match: r.RaftLog.offset - 1,
			Next:  lastIndex + 1,
		}
	}
	r.Step(r.nilProposeMessage())
	// NOTE: Leader should propose a noop entry on its term
}
