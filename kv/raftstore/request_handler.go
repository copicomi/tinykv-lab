package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
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

func (d *peerMsgHandler) handleAdminChangePeer(change_peer *raft_cmdpb.ChangePeerRequest, wb *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	reply := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		},
	}
	switch change_peer.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		d.handleAdminChangePeerAddNode(change_peer, wb)
	case eraftpb.ConfChangeType_RemoveNode:
		d.handleAdminChangePeerRemoveNode(change_peer, wb)
	default:
		log.Panicf("[%s] unknown change peer type %v", d.Tag, change_peer.ChangeType)
	}
	reply.AdminResponse.ChangePeer.Region = d.Region()

	return reply
}

func (d *peerMsgHandler) handleAdminChangePeerAddNode(change_peer *raft_cmdpb.ChangePeerRequest, wb *engine_util.WriteBatch) {
	if util.FindPeer(d.Region(), change_peer.Peer.Id) != nil {
		return
	}
	util.AddPeer(d.Region(), change_peer.Peer)
	d.insertPeerCache(change_peer.Peer)
	d.Region().RegionEpoch.ConfVer++
	meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
}

func (d *peerMsgHandler) handleAdminChangePeerRemoveNode(change_peer *raft_cmdpb.ChangePeerRequest, wb *engine_util.WriteBatch) {
	peer := util.RemovePeer(d.Region(), change_peer.Peer.Id)
	if peer == nil {
		return
	}
	if peer.Id == d.PeerId() {
		d.destroyPeer()
		return
	}
	d.removePeerCache(peer.Id)
	d.Region().RegionEpoch.ConfVer++
	meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
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
