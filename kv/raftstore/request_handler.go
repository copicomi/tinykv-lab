package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

func (d *peerMsgHandler) handleAdminCompactLog(compact_log *raft_cmdpb.CompactLogRequest, wb *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	reply := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		},
	}
	if compact_log.GetCompactIndex() < d.peerStorage.applyState.TruncatedState.Index {
		return reply
	}
	d.peerStorage.applyState.TruncatedState.Index = compact_log.GetCompactIndex()
	d.peerStorage.applyState.TruncatedState.Term = compact_log.GetCompactTerm()
	if err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		log.Panic(err)
	}
	d.ScheduleCompactLog(compact_log.GetCompactIndex())
	return reply
}

func (d *peerMsgHandler) handleAdminTransferLeader(transfer_leader *raft_cmdpb.TransferLeaderRequest) *raft_cmdpb.RaftCmdResponse {
	reply := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		},
	}
	d.RaftGroup.TransferLeader(transfer_leader.GetPeer().Id)
	return reply
}

func (d *peerMsgHandler) handleAdminSplit(split *raft_cmdpb.SplitRequest, wb *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	return nil
}

func (d *peerMsgHandler) handleNormalGet(get *raft_cmdpb.GetRequest) (*raft_cmdpb.GetResponse, error) {
	ans, err := engine_util.GetCF(d.ctx.engine.Kv, get.GetCf(), get.GetKey())
	if err != nil {
		return nil, err
	}
	return &raft_cmdpb.GetResponse{Value: ans}, nil
}

func (d *peerMsgHandler) handleNormalPut(put *raft_cmdpb.PutRequest, wb *engine_util.WriteBatch) *raft_cmdpb.PutResponse {
	engine_util.PutCF(d.ctx.engine.Kv, put.GetCf(), put.GetKey(), put.GetValue())
	return &raft_cmdpb.PutResponse{}
}

func (d *peerMsgHandler) handleNormalDelete(delete *raft_cmdpb.DeleteRequest, wb *engine_util.WriteBatch) *raft_cmdpb.DeleteResponse {
	engine_util.DeleteCF(d.ctx.engine.Kv, delete.GetCf(), delete.GetKey())
	return &raft_cmdpb.DeleteResponse{}
}

func (d *peerMsgHandler) handleNormalSnap(snap *raft_cmdpb.SnapRequest, wb *engine_util.WriteBatch, p *proposal) *raft_cmdpb.SnapResponse {
	// TODO: snapshot (2C)
	if p != nil {
		p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	}
	return &raft_cmdpb.SnapResponse{Region: d.Region()}
}
