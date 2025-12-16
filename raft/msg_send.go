package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, grant bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  !grant,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendBeat() {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHup() {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}
