package raft

import "github.com/pingcap-incubator/tinykv/log"

func (r *Raft) startElection() {
	// TODO: 一轮选举失败后，不需要再增加 Term
	r.becomeCandidate()
	log.Warningf("[%d] S%d becomeCandidate", r.Term, r.id)
	r.bcastRequestVote()
	if r.haveGotMajorVotes() {
		r.becomeLeader()
		log.Errorf("[%d] S%d becomeLeader", r.Term, r.id)
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
		r.electionElapsed = 0
		r.randomExtraElectionTime = randInt(0, r.electionTimeout+1)
	}
}
