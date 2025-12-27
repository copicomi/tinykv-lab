package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	if r.State != StateLeader {
		return false
	}
	var entries []*pb.Entry
	var err error
	if r.RaftLog.LastIndex() >= r.Prs[to].Next { // 正常更新
		entries, err = r.RaftLog.nextEntries(r.Prs[to].Next)
		if err == ErrCompacted { // 发送快照
			log.Warningf("%d try send snapshot to %d", r.id, to)
			return r.sendSnapshot(to)
		}
	} else { // 心跳
		entries = nil
	}
	prev_log_term, _ := r.RaftLog.Term(r.Prs[to].Next - 1)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Entries: entries,
		LogTerm: prev_log_term,
		Index:   r.Prs[to].Next - 1,
		Commit:  r.RaftLog.committed,
	}
	mDebug(r, "send Append to %d, index=%d, len=%d", msg.To, msg.Index, len(entries))
	r.send(msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, success bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  !success,
		Index:   r.RaftLog.LastIndex(),
	}
	mDebug(r, "send AppendResponse to %d, index=%d", msg.To, msg.Index)
	r.send(msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := r.RaftLog.committed
	if pr, ok := r.Prs[to]; ok && pr.Match == 0 {
		// 我们只在 AddNode 时将 Match 设为 0
		// 此时将 commit 设为 0，用于指示 worker 初始化 peer
		// 这里的判断见 kv/raftstore/util.go:IsInitialMsg()
		commit = 0
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	}
	r.send(msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: term,
	}
	r.send(msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, grant bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  !grant,
	}
	r.send(msg)
}

func (r *Raft) sendSnapshot(to uint64) bool {
	snapshot, err := r.RaftLog.storage.Snapshot()

	if err == ErrSnapshotTemporarilyUnavailable {
		return false
	}
	if err != nil {
		log.Panic(err)
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.send(msg)
	log.Warningf("unimplemented sendSnapshot")
	return true
}

func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.send(msg)
}

func (r *Raft) sendTransferLeader(to uint64, tranferLead uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTransferLeader,
		From:    tranferLead,
		To:      to,
		// term 0 because it's a local message
	}
	r.send(msg)
}
