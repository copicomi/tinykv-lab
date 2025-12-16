package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) handleBeat(m pb.Message) {
	if r.State == StateLeader {
		r.bcastHeartbeat()
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	granted := false
	if m.Term < r.Term {
		granted = false
	} else if r.Vote == None || r.Vote == m.From {
		granted = true
		r.Vote = m.From
	}
	r.sendRequestVoteResponse(m.From, granted)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	if r.haveGotMajorVotes() {
		r.becomeLeader()
	}
}

func (r *Raft) handleHup(m pb.Message) {
	if r.State != StateLeader {
		r.startElection()
	}
}
