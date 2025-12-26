package raft

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Lead = lead
	r.leadTransferee = None
	if term > r.Term {
		r.Vote = None
	}
	r.Term = term
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Vote = None
	r.leadTransferee = None
	// 自己不一定在 peer 集群里
	for _, peer := range r.peers {
		if peer == r.id {
			r.votes[peer] = true
			r.Vote = r.id
		} else {
			r.votes[peer] = false
		}
	}
	r.rejects_count = 0
	r.Term++
	// log.Warningf("[%d] S%d becomeCandidate", r.Term, r.id)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// log.Debugf("[%d] S%d becomeLeader", r.Term, r.id)
	// Your Code Here (2A).
	r.State = StateLeader
	r.leadTransferee = None
	r.Lead = r.id
	r.Vote = r.id
	lastIndex := r.RaftLog.LastIndex()
	for _, id := range r.peers {
		r.Prs[id] = &Progress{
			Match: r.RaftLog.snapshotIndex,
			Next:  lastIndex + 1,
		}
	}
	r.Step(r.nilProposeMessage())
	// NOTE: Leader should propose a noop entry on its term
}
