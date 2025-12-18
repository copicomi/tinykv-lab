package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

func (rf *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		rf.handleBeat(m)
	case pb.MessageType_MsgAppendResponse:
		rf.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgPropose:
		rf.handlePropose(m)
	case pb.MessageType_MsgHeartbeatResponse:
		rf.handleHeartbeatResponse(m)
	}
}

func (rf *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		rf.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		rf.handleAppendEntries(m)
	case pb.MessageType_MsgHup:
		rf.handleHup(m)
	case pb.MessageType_MsgHeartbeat:
		rf.handleHeartbeat(m)
	}
}

func (rf *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		rf.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		rf.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		rf.handleRequestVote(m)
	case pb.MessageType_MsgHup:
		rf.handleHup(m)
	}
}
