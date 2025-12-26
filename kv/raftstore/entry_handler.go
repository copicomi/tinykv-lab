package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

func (d *peerMsgHandler) processEntry(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	//log.Infof("[%s] committed entry %d ", d.Tag, entry.Index)
	if len(entry.Data) == 0 {
		return
	}
	switch entry.EntryType {
	case eraftpb.EntryType_EntryNormal:
		d.handleNormalEntry(entry, wb)
	case eraftpb.EntryType_EntryConfChange:
		d.handleConfChangeEntry(entry, wb)
	}
}

func (d *peerMsgHandler) handleNormalEntry(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	msg := &raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(entry.Data); err != nil {
		log.Panic(err)
	}
	log.Debug(msg)
	if msg.AdminRequest != nil {
		d.handleAdminCmdRequest(msg.AdminRequest, wb)
	} else {
		d.handleNormalCmdRequest(entry, msg, wb)
	}
}

func (d *peerMsgHandler) handleConfChangeEntry(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {

}

func (d *peerMsgHandler) handleNormalCmdRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) {
	reply := &raft_cmdpb.RaftCmdResponse{
		Responses: make([]*raft_cmdpb.Response, 0),
		Header:    &raft_cmdpb.RaftResponseHeader{},
	}
	p := d.FindProposal(entry.Index, entry.Term)
	for _, req := range msg.Requests {
		resp, err := d.handleRaftRequest(req, wb, p)
		if err != nil {
			reply = ErrResp(err)
			break
		}
		reply.Responses = append(reply.Responses, resp)
	}
	if p != nil {
		p.cb.Done(reply)
	}
	d.peerStorage.applyState.AppliedIndex = entry.Index
	wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	// TODO: 是否应该分批写入 DB？
}
func (d *peerMsgHandler) handleAdminCmdRequest(adminReq *raft_cmdpb.AdminRequest, wb *engine_util.WriteBatch) {
	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.handleAdminCompactLog(adminReq.GetCompactLog(), wb)
	default:
		log.Warningf("unknown admin command %v", adminReq.CmdType)
	}
}

func (d *peerMsgHandler) handleRaftRequest(req *raft_cmdpb.Request,
	wb *engine_util.WriteBatch, p *proposal) (*raft_cmdpb.Response, error) {
	resp := &raft_cmdpb.Response{
		CmdType: req.CmdType,
	}
	var err error
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		resp.Get, err = d.handleNormalGet(req.GetGet())
	case raft_cmdpb.CmdType_Put:
		resp.Put = d.handleNormalPut(req.GetPut(), wb)
	case raft_cmdpb.CmdType_Delete:
		resp.Delete = d.handleNormalDelete(req.GetDelete(), wb)
	case raft_cmdpb.CmdType_Snap:
		resp.Snap = d.handleNormalSnap(req.GetSnap(), wb, p)
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (d *peerMsgHandler) proposeRequestMessage(msg *raft_cmdpb.RaftCmdRequest) {
	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	if err = d.RaftGroup.Propose(data); err != nil {
		log.Panic(err)
	}
}
func (d *peerMsgHandler) proposeRequestNormal(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	d.appendProposal(cb)
	d.proposeRequestMessage(msg)
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	if msg.AdminRequest == nil {
		log.Warning("msg must contain admin request")
		return
	}
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		// 没有 callback 函数
		d.proposeRequestMessage(msg)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		reply := d.handleAdminTransferLeader(msg.AdminRequest.TransferLeader)
		cb.Done(reply)
	}
}
func (d *peerMsgHandler) FindProposal(index, term uint64) *proposal {
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		d.proposals = d.proposals[1:]
		if p.index > index {
			break
		} else if p.index == index {
			if p.term < term {
				p.cb.Done(ErrRespStaleCommand(d.Term()))
			} else if p.term == term {
				return p
			} else {
				break
			}
		}
	}
	return nil
}
func (d *peerMsgHandler) appendProposal(cb *message.Callback) {
	proposal := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	//log.Infof("[%s] Appending proposal %d", d.Tag, proposal.index)
	d.proposals = append(d.proposals, proposal)
}
