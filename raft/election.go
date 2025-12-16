package raft

func (r *Raft) startElection() {
	// TODO: 一轮选举失败后，不需要再增加 Term
	r.becomeCandidate()
	r.bcastRequestVote()
	if r.haveGotMajorVotes() {
		r.becomeLeader()
	}
}

func (r *Raft) haveGotMajorVotes() bool {
	if r.State != StateCandidate {
		return false
	}
	grantedVotes := 0
	for _, granted := range r.votes {
		if granted {
			grantedVotes++
		}
	}
	return grantedVotes*2 > len(r.peers)
}

func (r *Raft) bcastHeartbeat() {
	if r.State == StateLeader {
		for _, peer := range r.peers {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	}
}

func (r *Raft) bcastRequestVote() {
	for _, peer := range r.peers {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}
