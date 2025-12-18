package raft

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
	for _, peer := range r.peers {
		r.votes[peer] = false
	}
	r.votes[r.id] = true
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None
	lastIndex := r.RaftLog.LastIndex()
	for _, id := range r.peers {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}
	r.Step(r.nilProposeMessage())
	// NOTE: Leader should propose a noop entry on its term
}
