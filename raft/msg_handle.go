package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	mDebug(r, "handle Append From %d", m.From)
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
	mDebug(r, "handle AppendResp From %d", m.From)
	if m.Reject {
		r.Prs[m.From].Next--
		//TODO: 大步回退

		if r.Prs[m.From].Next < r.RaftLog.snapshotIndex {
			log.Errorf("raft %d: next of peer %d = %d", r.id, m.From, r.Prs[m.From].Next)
		}
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
	r.Prs[m.From].Next = r.Prs[m.From].Match + 1
	mDebug(r, "update match[%d] to %d", m.From, r.Prs[m.From].Match)
	// If transferring leadership and transferee caught up, trigger timeout now.
	if r.isReadyToTransferLeader(m.From) {
		r.sendTimeoutNow(m.From)
	}

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	mDebug(r, "handle Heartbeat From %d", m.From)
	r.sendHeartbeatResponse(m.From)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	mDebug(r, "handle HeartbeatResp From %d, m.index=%d, match=%d", m.From, m.Index, r.Prs[m.From].Match)
	// mDebug(r, "m.index=%d, l.lastindex=%d", m.Index, r.RaftLog.LastIndex())
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
	r.Prs[m.From].Next = r.Prs[m.From].Match + 1
	r.sendAppend(m.From)
	// Heartbeat acknowledged; if transferee is caught up, trigger leadership transfer.
	if r.isReadyToTransferLeader(m.From) {
		r.sendTimeoutNow(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	mDebug(r, "handle Snapshot From %d", m.From)
	success := r.InstallSnapshot(m.Snapshot)
	r.sendAppendResponse(m.From, success)
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
		// log.Infof("[%d] S%d Vote to %d", r.Term, r.id, r.Vote)
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
		mDebug(r, "got vote from %d", m.From)
	}
	if r.haveGotMajorVotes() {
		r.becomeLeader()
		mDebug(r, "become leader")
	}
}

func (r *Raft) handleHup(m pb.Message) {
	r.startElection()
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	if r.leadTransferee != None { // leader transfering, reject new proposals
		log.Warning("raft dropped proposal while transferring leader")
		return
	}
	for _, ent := range m.Entries {
		if ent.EntryType == pb.EntryType_EntryConfChange {
			// Only one pending conf change is allowed at a time.
			if r.PendingConfIndex != 0 && r.RaftLog.committed < r.PendingConfIndex {
				return
			}
			r.PendingConfIndex = r.RaftLog.LastIndex() + 1
		}
		newEnt := pb.Entry{
			EntryType: ent.EntryType,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      ent.Data,
		}
		r.RaftLog.append(newEnt)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.bcastAppend()
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	// Your Code Here (3A).
	target := m.From
	if target == None || !r.isInPeers(target, r.peers) {
		return
	}

	if r.State != StateLeader {
		if r.Lead != None { // forward to leader
			r.sendTransferLeader(r.Lead, target)
		}
		return
	}

	// Ignore requests to self but clear any pending transfer so proposals can proceed.
	if target == r.id {
		r.leadTransferee = None
		return
	}

	// If a different transfer is pending, override it with the new target.
	if r.leadTransferee == target {
		return
	}
	r.leadTransferee = target
	r.transferElapsed = 0

	if r.isReadyToTransferLeader(target) {
		r.sendTimeoutNow(target)
	} else {
		// Ask target to catch up first.
		r.sendAppend(target)
	}

}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	// Your Code Here (3A).
	if !r.isInPeers(r.id, r.peers) {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		From:    r.id,
		To:      r.id,
	}
	r.Step(msg)
}
