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

func (d *peerMsgHandler) handleNormalCmdRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) {
	reply := &raft_cmdpb.RaftCmdResponse{
		Responses: make([]*raft_cmdpb.Response, 0),
		Header:    &raft_cmdpb.RaftResponseHeader{},
	}
	p := d.FindProposal(entry.Index, entry.Term)
	for _, req := range msg.Requests {
		resp, err := d.handleRaftRequest(req, wb)
		if err != nil {
			// read fail
			if p != nil {
				p.cb.Done(ErrResp(err))
			}
		}
		reply.Responses = append(reply.Responses, resp)
		if p != nil && req.CmdType == raft_cmdpb.CmdType_Snap {
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
	}
	if p != nil {
		p.cb.Done(reply)
	}
	d.peerStorage.applyState.AppliedIndex = entry.Index
	wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
}
func (d *peerMsgHandler) handleAdminCmdRequest(adminReq *raft_cmdpb.AdminRequest, wb *engine_util.WriteBatch) {
	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compact_log := adminReq.GetCompactLog()
		if compact_log.GetCompactIndex() < d.peerStorage.applyState.TruncatedState.Index {
			return
		}
		d.peerStorage.applyState.TruncatedState.Index = compact_log.GetCompactIndex()
		d.peerStorage.applyState.TruncatedState.Term = compact_log.GetCompactTerm()
		if err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Panic(err)
		}
		d.ScheduleCompactLog(compact_log.GetCompactIndex())
	default:
		log.Warningf("unknown admin command %v", adminReq.CmdType)
	}
}

func (d *peerMsgHandler) handleRaftRequest(req *raft_cmdpb.Request,
	wb *engine_util.WriteBatch) (*raft_cmdpb.Response, error) {
	resp := &raft_cmdpb.Response{}
	resp.CmdType = req.CmdType
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		req_get := req.GetGet()
		ans, err := engine_util.GetCF(d.ctx.engine.Kv, req_get.GetCf(), req_get.GetKey())
		if err != nil {
			return nil, err
		}
		resp.Get = &raft_cmdpb.GetResponse{Value: ans}
	case raft_cmdpb.CmdType_Put:
		req_put := req.GetPut()
		wb.SetCF(req_put.GetCf(), req_put.GetKey(), req_put.GetValue())
		resp.Put = &raft_cmdpb.PutResponse{}
	case raft_cmdpb.CmdType_Delete:
		req_delete := req.GetDelete()
		wb.DeleteCF(req_delete.GetCf(), req_delete.GetKey())
		resp.Delete = &raft_cmdpb.DeleteResponse{}
	case raft_cmdpb.CmdType_Snap:
		resp.Snap = &raft_cmdpb.SnapResponse{Region: d.Region()}
		// TODO: snapshot (2C)
	}
	return resp, nil
}
func (d *peerMsgHandler) proposeRequestNormal(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	d.appendProposal(cb)
	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	if err = d.RaftGroup.Propose(data); err != nil {
		log.Panic(err)
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest) {
	if msg.AdminRequest == nil {
		log.Warning("msg must contain admin request")
		return
	}
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			log.Panic("error Marshal admin request", err)
		}
		if err := d.RaftGroup.Propose(data); err != nil {
			log.Panic(err)
		}
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
