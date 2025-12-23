package raft

func (r *Raft) startElection() {
	// TODO: 一轮选举失败后，不需要再增加 Term
	r.electionElapsed = 0
	r.randomExtraElectionTime = randInt(0, r.electionTimeout)
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

func (r *Raft) tickLeader() {
	r.electionElapsed++
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.bcastHeartbeat()
	}
}

func (r *Raft) tickNotLeader() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout+r.randomExtraElectionTime {
		r.startElection()
	}
}
