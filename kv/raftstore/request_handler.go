package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

func (d *peerMsgHandler) HandleAdminSplit(split *raft_cmdpb.SplitRequest, wb *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	// 校验 split key 是否仍在当前 Region 范围内（幂等保护）
	if err := util.CheckKeyInRegion(split.GetSplitKey(), d.Region()); err != nil {
		return ErrResp(err)
	}

	// 新旧 Region 的 peer 数量必须一致
	if len(d.Region().GetPeers()) != len(split.GetNewPeerIds()) {
		return ErrRespStaleCommand(d.Term())
	}

	// 更新旧 Region 的版本号（Split 触发 Version 增加）
	oldRegion := d.Region()
	oldRegion.RegionEpoch.Version++

	// 构造新 Region 元信息
	newRegion := &metapb.Region{
		Id:       split.GetNewRegionId(),
		StartKey: append([]byte{}, split.GetSplitKey()...),
		EndKey:   append([]byte{}, oldRegion.GetEndKey()...),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: oldRegion.RegionEpoch.ConfVer,
			Version: oldRegion.RegionEpoch.Version,
		},
		Peers: make([]*metapb.Peer, 0, len(oldRegion.GetPeers())),
	}

	// 为新 Region 生成与旧 Region 对应 store 的 peers（使用调度器分配的 peer ids）
	for i, p := range oldRegion.GetPeers() {
		newRegion.Peers = append(newRegion.Peers, &metapb.Peer{Id: split.GetNewPeerIds()[i], StoreId: p.GetStoreId()})
	}

	// 更新 storeMeta 的 Region 范围映射
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.regionRanges.Delete(&regionItem{region: oldRegion})
	oldRegion.EndKey = append([]byte{}, split.GetSplitKey()...)
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: oldRegion})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	storeMeta.regions[newRegion.Id] = newRegion
	storeMeta.Unlock()

	// 持久化新旧 Region 状态
	meta.WriteRegionState(wb, oldRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(wb, newRegion, rspb.PeerState_Normal)

	// 在当前 store 上创建并启动新 Region 对应的 peer
	peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.schedulerTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Panic(err)
	}
	d.ctx.router.register(peer)
	d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})

	// Leader 触发心跳，用于让调度器快速感知分裂
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		d.notifyHeartbeatScheduler(newRegion, peer)
	}

	// 构造响应（回调在上层未直接处理，这里返回标准响应对象）
	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{newRegion, oldRegion}},
		},
	}
}

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
	// Bump conf version before potentially destroying this peer so epoch stays
	// consistent across all replicas that apply the conf change.
	d.Region().RegionEpoch.ConfVer++
	if peer.Id == d.PeerId() {
		d.destroyPeer()
		return
	}
	d.removePeerCache(peer.Id)
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
	// 更新 SizeDiffHint，避免 split+crash 测试在 Delete 阶段卡住
	d.SizeDiffHint += uint64(len(put.GetKey()) + len(put.GetValue()))
	return &raft_cmdpb.PutResponse{}
}

func (d *peerMsgHandler) handleNormalDelete(delete *raft_cmdpb.DeleteRequest, wb *engine_util.WriteBatch) *raft_cmdpb.DeleteResponse {
	engine_util.DeleteCF(d.ctx.engine.Kv, delete.GetCf(), delete.GetKey())
	// 粗略减去 key 尺寸，保持触发频率
	if d.SizeDiffHint > 0 {
		k := uint64(len(delete.GetKey()))
		if d.SizeDiffHint > k {
			d.SizeDiffHint -= k
		} else {
			d.SizeDiffHint = 0
		}
	}
	return &raft_cmdpb.DeleteResponse{}
}

func (d *peerMsgHandler) handleNormalSnap(snap *raft_cmdpb.SnapRequest, wb *engine_util.WriteBatch, p *proposal) *raft_cmdpb.SnapResponse {
	// TODO: snapshot (2C)
	if p != nil {
		p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	}
	return &raft_cmdpb.SnapResponse{Region: d.Region()}
}
