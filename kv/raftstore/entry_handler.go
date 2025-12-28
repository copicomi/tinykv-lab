package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
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
	// log.Infof("[%s] processing entry index=%d, has admin request: %v", d.Tag, entry.Index, msg.AdminRequest != nil)
	if msg.AdminRequest != nil {
		d.handleAdminCmdRequest(msg.AdminRequest, wb)
	} else {
		d.handleNormalCmdRequest(entry, msg, wb)
	}
}

func (d *peerMsgHandler) handleConfChangeEntry(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	// TODO: Commit 完成后更新配置
	cc := eraftpb.ConfChange{}
	if err := cc.Unmarshal(entry.Data); err != nil {
		log.Panic(err)
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(cc.Context); err != nil {
		log.Panic(err)
	}
	reply := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
	}
	p := d.FindProposal(entry.Index, entry.Term)
	if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
		reply = ErrResp(err)
	} else {
		// log.Warningf("[%s] conf change Epoch %+v", d.Tag, msg.Header.GetRegionEpoch())
		d.RaftGroup.ApplyConfChange(cc)
		reply = d.handleAdminChangePeer(msg.AdminRequest.GetChangePeer(), wb)
		d.notifyHeartbeatScheduler(d.Region(), d.peer)
	}
	if p != nil {
		p.cb.Done(reply)
	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) handleNormalCmdRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) {
	reply := &raft_cmdpb.RaftCmdResponse{
		Responses: make([]*raft_cmdpb.Response, 0),
		Header:    &raft_cmdpb.RaftResponseHeader{},
	}
	p := d.FindProposal(entry.Index, entry.Term)
	if p == nil {
		// log.Warnf("[%s] proposal not found for entry index=%d term=%d", d.Tag, entry.Index, entry.Term)
	}
	if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
		reply = ErrResp(err)
		if p != nil {
			p.cb.Done(reply)
		}
		return
	}
	for _, req := range msg.Requests {
		resp, err := d.handleRaftRequest(req, wb, p)
		if err != nil {
			reply = ErrResp(err)
			break
		}
		reply.Responses = append(reply.Responses, resp)
		// log.Infof("[%s] calling callback for entry index=%d", d.Tag, entry.Index)
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
	case raft_cmdpb.AdminCmdType_Split:
		d.HandleAdminSplit(adminReq.GetSplit(), wb)
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
		if err := util.CheckKeyInRegion(req.GetGet().GetKey(), d.Region()); err != nil {
			return nil, err
		}
		resp.Get, err = d.handleNormalGet(req.GetGet())
	case raft_cmdpb.CmdType_Put:
		if err := util.CheckKeyInRegion(req.GetPut().GetKey(), d.Region()); err != nil {
			return nil, err
		}
		resp.Put = d.handleNormalPut(req.GetPut(), wb)
	case raft_cmdpb.CmdType_Delete:
		if err := util.CheckKeyInRegion(req.GetDelete().GetKey(), d.Region()); err != nil {
			return nil, err
		}
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
		log.Warningf("%v", err)
		return
	}
}
func (d *peerMsgHandler) proposeConfChangeRequestMessage(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
		return
	}
	cp := msg.AdminRequest.ChangePeer
	cc := eraftpb.ConfChange{
		ChangeType: cp.ChangeType,
		NodeId:     cp.Peer.Id,
		Context:    data,
	}
	d.appendProposal(cb)
	if err = d.RaftGroup.ProposeConfChange(cc); err != nil {
		log.Panic(err)
	}
}

func (d *peerMsgHandler) proposeRequestNormal(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	d.appendProposal(cb)
	d.proposeRequestMessage(msg)
}

func (d *peerMsgHandler) proposeConfChangeWith2Peers(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) bool {
	req := &raft_cmdpb.RaftCmdRequest{}
	// Handle the case where leader is being removed with only 2 peers in unreliable network.
	// In this case, reject the propose and initiate a leadership transfer.
	if req.AdminRequest != nil && req.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		peers := d.Region().GetPeers()
		if len(peers) == 2 && req.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode {
			removePeerID := req.AdminRequest.ChangePeer.Peer.Id
			if removePeerID == d.PeerId() {
				// Leader is being removed with only 2 peers in the region.
				// In unreliable network, we should:
				// 1. Reject this propose
				// 2. Initiate leadership transfer to the other peer
				// This avoids the situation where:
				// - Leader removes itself
				// - But the other peer doesn't receive the conf change due to packet loss
				// - The other peer will try to elect but can't get vote from the removed peer
				otherPeer := d.getOtherPeer(removePeerID, peers)
				if otherPeer != nil {
					log.Infof("%s initiating leadership transfer to peer %d before rejecting self-remove", d.Tag, otherPeer.Id)
					d.RaftGroup.TransferLeader(otherPeer.Id)
				}
				// Reject this propose, client will retry
				return true
			}
		}
	}
	return false
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
		// log.Warningf("[%s] proposing transfer leader %+v", d.Tag, msg.AdminRequest.TransferLeader)
		reply := d.handleAdminTransferLeader(msg.AdminRequest.TransferLeader)
		cb.Done(reply)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		if d.proposeConfChangeWith2Peers(msg, cb) {
			return
		}
		//log.Warningf("[%s] proposing conf change %+v", d.Tag, msg.AdminRequest.ChangePeer)
		d.proposeConfChangeRequestMessage(msg, cb)
	case raft_cmdpb.AdminCmdType_Split:
		if err := util.CheckKeyInRegion(msg.AdminRequest.Split.GetSplitKey(), d.Region()); err != nil {
			reply := ErrResp(err)
			cb.Done(reply)
			return
		}
		if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
			reply := ErrResp(err)
			cb.Done(reply)
			return
		}
		d.appendProposal(cb)
		d.proposeRequestMessage(msg)

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
	// log.Infof("[%s] appending proposal index=%d term=%d", d.Tag, proposal.index, proposal.term)
	d.proposals = append(d.proposals, proposal)
}
