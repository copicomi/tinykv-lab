package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	prev_log_index, prev_log_term, entries := m.Index, m.LogTerm, m.Entries
	success := false

	if r.isMatchPrevLog(prev_log_index, prev_log_term) {
		success = true
		r.RaftLog.appendEntries(entries, prev_log_index)
		r.RaftLog.committed = max(r.RaftLog.committed, min(m.Commit, prev_log_index+uint64(len(entries))))
		// mDebug(r, "commit=%d, index=%d", r.RaftLog.committed, r.RaftLog.LastIndex())
	}

	r.sendAppendResponse(m.From, success)

}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Reject {
		r.Prs[m.From].Next--
		//TODO: 大步回退

		if r.Prs[m.From].Next < 0 {
			log.Errorf("raft %d: next of peer %d = %d", r.id, m.From, r.Prs[m.From].Next)
		}
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
	r.Prs[m.From].Next = r.Prs[m.From].Match + 1

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.sendHeartbeatResponse(m.From)
	r.electionElapsed = 0
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// mDebug(r, "m.index=%d, l.lastindex=%d", m.Index, r.RaftLog.LastIndex())
	if m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
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
	// mDebug(r, "now vote: %d", r.Vote)
	if !r.hasNewerLogThan(m.LogTerm, m.Index) && (r.Vote == None || r.Vote == m.From) {
		granted = true
		r.Vote = m.From
		log.Infof("[%d] S%d Vote to %d", r.Term, r.id, r.Vote)
	}
	r.sendRequestVoteResponse(m.From, granted)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	if m.Reject {
		r.rejects_count++
		if r.rejects_count*2 > len(r.peers) {
			r.becomeFollower(r.Term, None)
		}
	} else {
		mInfo(r, "got vote from %d", m.From)
	}
	if r.haveGotMajorVotes() {
		r.becomeLeader()
		mInfo(r, "become leader")
	}
}

func (r *Raft) handleHup(m pb.Message) {
	r.startElection()
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State == StateLeader {
		for _, entry := range m.Entries {
			entry := &pb.Entry{
				Term:  r.Term,
				Index: r.RaftLog.LastIndex() + 1,
				Data:  entry.Data,
			}
			r.RaftLog.append(*entry)
		}
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
		r.bcastAppend()
	}
}
