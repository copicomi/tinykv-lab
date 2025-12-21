package raft

func (r *Raft) bcastHeartbeat() {
	if r.State == StateLeader {
		r.heartbeatElapsed = 0
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

func (r *Raft) bcastAppend() {
	for _, to := range r.peers {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}
func (r *Raft) bcastApply() {

}
