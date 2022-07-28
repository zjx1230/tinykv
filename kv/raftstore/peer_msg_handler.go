package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) responseProposal(entry *pb.Entry, handle func(*proposal)) {
	idx := -1
	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()
	for index, prop := range d.proposals {
		if prop.index > entry.Index {
			break
		}

		if prop.index == entry.Index && prop.term == entry.Term {
			handle(prop)
			idx = index
			break
		}

		if prop.index == entry.Index && prop.term < entry.Term {
			NotifyStaleReq(prop.term, prop.cb)
			idx = index
		}
	}

	if idx != -1 {
		d.proposals = d.proposals[idx+1:]
	}
}

func (d *peerMsgHandler) findPeerIndex(peerId uint64) int {
	for index, p := range d.peerStorage.region.Peers {
		if p.Id == peerId {
			return index
		}
	}
	return -1
}

func (d *peerMsgHandler) apply(rd raft.Ready, term uint64) {
	kvWB := new(engine_util.WriteBatch)
	for _, entry := range rd.CommittedEntries {
		if d.stopped {
			return
		}

		if entry.Index > d.peerStorage.applyState.AppliedIndex {
			d.peerStorage.applyState.AppliedIndex = entry.Index
		}

		if entry.EntryType == pb.EntryType_EntryApply {
			resp := newCmdResp()
			resp.Header.CurrentTerm = term
			//d.ctx.storeMeta.Lock()
			//cb, index := d.getCorrespondingCallBack(&entry)
			//d.proposals = d.proposals[index+1:]
			//d.ctx.storeMeta.Unlock()
			d.processApplyEntry(&entry, kvWB, resp)
			d.responseProposal(&entry, func(p *proposal) {
				if p.cb != nil {
					p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				}
				p.cb.Done(resp)
			})
		} else if entry.EntryType == pb.EntryType_EntryConfChange {
			var confChange pb.ConfChange
			err := confChange.Unmarshal(entry.Data)
			if err != nil {
				panic(err)
			}
			var context raft_cmdpb.RaftCmdRequest
			err = context.Unmarshal(confChange.Context)
			if err != nil {
				panic(err)
			}

			region := d.Region()
			err = util.CheckRegionEpoch(&context, region, true)
			if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
				d.responseProposal(&entry, func(p *proposal) {
					p.cb.Done(ErrResp(errEpochNotMatching))
				})
				return
			}

			if confChange.ChangeType == pb.ConfChangeType_AddNode {
				fmt.Printf("ConfChangeType_AddNode\n")
				if d.findPeerIndex(confChange.NodeId) == -1 {
					d.peerStorage.region.RegionEpoch.ConfVer++
					d.peerStorage.region.Peers = append(d.peerStorage.region.Peers, context.AdminRequest.ChangePeer.Peer)
					d.ctx.storeMeta.Lock()
					d.ctx.storeMeta.regions[d.regionId] = d.Region()
					d.ctx.storeMeta.Unlock()
					d.insertPeerCache(context.AdminRequest.ChangePeer.Peer)
				}
			} else if confChange.ChangeType == pb.ConfChangeType_RemoveNode {
				fmt.Printf("ConfChangeType_RemoveNode\n")
				if d.MaybeDestroy() && confChange.NodeId == d.PeerId() {
					d.destroyPeer()
					return
				}

				if index := d.findPeerIndex(confChange.NodeId); index != -1 {
					d.peerStorage.region.RegionEpoch.ConfVer++
					d.peerStorage.region.Peers = append(d.peerStorage.region.Peers[0:index], d.peerStorage.region.Peers[index+1:]...)
					d.ctx.storeMeta.Lock()
					d.ctx.storeMeta.regions[d.regionId] = d.Region()
					d.ctx.storeMeta.Unlock()
					d.removePeerCache(confChange.NodeId)
				}
			}
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			d.RaftGroup.ApplyConfChange(confChange)
			d.responseProposal(&entry, func(p *proposal) {
				p.cb.Done(&raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
						ChangePeer: &raft_cmdpb.ChangePeerResponse{},
					},
				})
			})
			if d.IsLeader() {
				d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			}
		} else {
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			meta.WriteApplyState(kvWB, d.regionId, d.peerStorage.applyState.AppliedIndex, &rspb.RaftTruncatedState{
				Index: d.peerStorage.applyState.TruncatedState.Index,
				Term:  d.peerStorage.applyState.TruncatedState.Term,
			})
		}
	}
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		res, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic(err)
		}

		if res != nil && !reflect.DeepEqual(res.PrevRegion, res.Region) {
			d.SetRegion(res.Region)
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[d.regionId] = res.Region
			d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: res.PrevRegion})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: res.Region})
			d.ctx.storeMeta.Unlock()
		}

		d.Send(d.ctx.trans, rd.Messages)
		// process Apply
		d.apply(rd, d.Term())
		d.RaftGroup.Advance(rd)
	}
}

func (d *peerMsgHandler) getCorrespondingCallBack(entry *pb.Entry) (*message.Callback, int) {
	if entry == nil {
		return nil, -1
	}

	for index, prop := range d.proposals {
		if prop.index == entry.Index && prop.term == entry.Term {
			return prop.cb, index
		}
	}
	return nil, -1
}

func (d *peerMsgHandler) handleNonAdminRequest(requests *raft_cmdpb.RaftCmdRequest, resps *raft_cmdpb.RaftCmdResponse) {
	curRegion := d.peerStorage.region
	err := util.CheckRegionEpoch(requests, curRegion, true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		resps.Header.Error = util.RaftstoreErrToPbError(errEpochNotMatching)
		return
	}

	for _, request := range requests.Requests {
		resp := &raft_cmdpb.Response{
			CmdType: request.CmdType,
		}
		//fmt.Printf("cmdtype: %v, ", request.CmdType)
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			//fmt.Printf("id: %d, CmdType_Get cmd: key: %v", d.peer.PeerId(), string((*request).Get.Key))
			err = util.CheckKeyInRegion(request.Get.Key, curRegion)
			if err != nil {
				//fmt.Printf("CheckKeyInRegion err: %v\n", err)
				resps.Header.Error = util.RaftstoreErrToPbError(err)
				return
			}
			val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, request.Get.Cf, request.Get.Key)
			if err != nil {
				log.Errorf("engine_util.GetCF error: %v\n", err)
			}
			resp.Get = &raft_cmdpb.GetResponse{
				Value: val,
			}
			resps.Responses = append(resps.Responses, resp)
		case raft_cmdpb.CmdType_Put:
			//fmt.Printf("id: %d, CmdType_Put cmd: key: %v, value: %v", d.peer.PeerId(), string((*request).Put.Key), string((*request).Put.Value))
			err := engine_util.PutCF(d.peerStorage.Engines.Kv, request.Put.Cf, request.Put.Key, request.Put.Value)
			if err != nil {
				log.Errorf("engine_util.PutCF error: %v\n", err)
			}
			resp.Put = &raft_cmdpb.PutResponse{}
			resps.Responses = append(resps.Responses, resp)
		case raft_cmdpb.CmdType_Delete:
			err = util.CheckKeyInRegion(request.Delete.Key, curRegion)
			if err != nil {
				resps.Header.Error = util.RaftstoreErrToPbError(err)
				return
			}
			err := engine_util.DeleteCF(d.peerStorage.Engines.Kv, request.Delete.Cf, request.Delete.Key)
			if err != nil {
				log.Errorf("engine_util.DeleteCF error: %v\n", err)
			}
			resp.Delete = &raft_cmdpb.DeleteResponse{}
			resps.Responses = append(resps.Responses, resp)
		case raft_cmdpb.CmdType_Snap:
			resp.Snap = &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			}
			resps.Responses = append(resps.Responses, resp)
		default:
			log.Panicf("unknown cmd type: %v\n", request.CmdType)
		}
	}
}

func (d *peerMsgHandler) handleAdminRequest(requests *raft_cmdpb.RaftCmdRequest, resps *raft_cmdpb.RaftCmdResponse, kvWB *engine_util.WriteBatch, entry *pb.Entry) {
	adminRequest := requests.AdminRequest
	resp := &raft_cmdpb.AdminResponse{
		CmdType: requests.AdminRequest.CmdType,
	}
	switch adminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
		if adminRequest.CompactLog.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
			meta.WriteApplyState(kvWB, d.regionId, entry.Index, &rspb.RaftTruncatedState{
				Index: adminRequest.CompactLog.CompactIndex,
				Term:  adminRequest.CompactLog.CompactTerm,
			})
			d.peerStorage.applyState.TruncatedState.Index = adminRequest.CompactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = adminRequest.CompactLog.CompactTerm
			d.ScheduleCompactLog(adminRequest.CompactLog.CompactIndex) // 异步地清理掉底层数据库存储的RaftLog
		}
	case raft_cmdpb.AdminCmdType_Split:
		curRegion := d.peerStorage.region
		err := util.CheckRegionEpoch(requests, curRegion, true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			fmt.Printf("err: %v\n", errEpochNotMatching)
			resps.Header.Error = util.RaftstoreErrToPbError(errEpochNotMatching)
			resps.AdminResponse = resp
			return
		}

		err = util.CheckKeyInRegion(adminRequest.Split.SplitKey, curRegion)
		if err != nil {
			fmt.Printf("CheckKeyInRegion err: %v\n", err)
			resps.Header.Error = util.RaftstoreErrToPbError(err)
			resps.AdminResponse = resp
			return
		}
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: curRegion})
		d.ctx.storeMeta.Unlock()
		// curRegion split into the left region and the right region
		curRegion.RegionEpoch.Version++
		leftRegion := &metapb.Region{
			Id:          curRegion.Id,
			StartKey:    curRegion.StartKey,
			EndKey:      adminRequest.Split.SplitKey,
			RegionEpoch: curRegion.RegionEpoch,
			Peers:       curRegion.Peers,
		}

		newPeers := make([]*metapb.Peer, 0)
		for i := range adminRequest.Split.NewPeerIds {
			newPeers = append(newPeers, &metapb.Peer{
				Id:      adminRequest.Split.NewPeerIds[i],
				StoreId: d.Region().Peers[i].StoreId,
			})
		}
		rightRegion := &metapb.Region{
			Id:       adminRequest.Split.NewRegionId,
			StartKey: adminRequest.Split.SplitKey,
			EndKey:   curRegion.EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: newPeers,
		}
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, rightRegion)
		if err != nil {
			panic(err)
		}
		d.SetRegion(leftRegion)
		meta.WriteRegionState(kvWB, leftRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, rightRegion, rspb.PeerState_Normal)
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.setRegion(rightRegion, newPeer)
		d.ctx.storeMeta.setRegion(leftRegion, d.peer)
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})
		d.ctx.storeMeta.Unlock()
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)

		d.ctx.router.register(newPeer)
		d.ctx.router.send(rightRegion.Id, message.Msg{Type: message.MsgTypeStart, RegionID: rightRegion.Id})
		resp.Split = &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{
				leftRegion,
				rightRegion,
			},
		}
	case raft_cmdpb.AdminCmdType_InvalidAdmin:

	case raft_cmdpb.AdminCmdType_TransferLeader:
		fmt.Printf("do nothing")
	default:
		log.Panicf("unknown cmd type: %v\n", adminRequest.CmdType)
	}
	resps.AdminResponse = resp
}

func (d *peerMsgHandler) processApplyEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch, resps *raft_cmdpb.RaftCmdResponse) {
	requests := new(raft_cmdpb.RaftCmdRequest)
	err := requests.Unmarshal(entry.Data)
	if err != nil {
		log.Panicf("request.Unmarshal error: %v\n", err)
	}

	if requests.Requests != nil && len(requests.Requests) != 0 {
		resps.Responses = make([]*raft_cmdpb.Response, 0)
		d.handleNonAdminRequest(requests, resps)
		meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
		meta.WriteApplyState(kvWB, d.regionId, d.peerStorage.applyState.AppliedIndex, &rspb.RaftTruncatedState{
			Index: d.peerStorage.applyState.TruncatedState.Index,
			Term:  d.peerStorage.applyState.TruncatedState.Term,
		})
	} else { // Admin Request
		d.handleAdminRequest(requests, resps, kvWB, entry)
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) appendProposal(cb *message.Callback) {
	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()
	d.proposals = append(d.proposals, &proposal{
		index: d.RaftGroup.Raft.RaftLog.LastIndex() + 1,
		term:  d.Term(),
		cb:    cb,
	})
	//fmt.Printf("id: %d, proposal, index: %d, term: %d, ", d.peer.PeerId(), d.proposals[len(d.proposals)-1].index, d.proposals[len(d.proposals)-1].term)
	//for i := range msg.Requests {
	//	req := msg.Requests[i]
	//	fmt.Printf("cmdtype: %v, ", req.CmdType)
	//	if req.CmdType == raft_cmdpb.CmdType_Put {
	//		fmt.Printf("put cmd: key: %v, value: %v", string((*req).Put.Key), string((*req).Put.Value))
	//	} else if req.CmdType == raft_cmdpb.CmdType_Get {
	//		fmt.Printf("get cmd: key: %v", string((*req).Get.Key))
	//	} else if req.CmdType == raft_cmdpb.CmdType_Snap {
	//		fmt.Printf("cmd: %v", *req.Snap)
	//	} else {
	//		fmt.Printf("cmd: %v", *req.Delete)
	//	}
	//	fmt.Printf("\n")
	//}
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	d.appendProposal(cb)
	data, err := msg.Marshal()
	if err != nil {
		log.Errorf("req: %v, req.Marshal() err: %v\n", msg, err)
	}
	//fmt.Printf("proposeRaftCommand, req: %v\n", req)
	err = d.RaftGroup.Propose(data)
	if err != nil {
		log.Errorf("d.RaftGroup.Propose err: %v\n", err)
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	adminRequest := msg.AdminRequest
	if adminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{
				CurrentTerm: d.Term(),
			},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			},
		})
		return
	} else if adminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
			return
		}

		if d.IsLeader() && len(d.peerStorage.region.Peers) == 2 && adminRequest.ChangePeer.Peer.Id == d.PeerId() { // two node, and want to remove the leader, reject it and transfer the leader
			if d.peerStorage.region.Peers[0].Id == d.PeerId() {
				d.RaftGroup.TransferLeader(d.peerStorage.region.Peers[1].Id)
			} else {
				d.RaftGroup.TransferLeader(d.peerStorage.region.Peers[0].Id)
			}
			return
		}
		d.appendProposal(cb)
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		err = d.RaftGroup.ProposeConfChange(pb.ConfChange{
			ChangeType: adminRequest.ChangePeer.ChangeType,
			NodeId:     adminRequest.ChangePeer.Peer.Id,
			Context:    context,
		})

		if err != nil {
			panic(err)
		}
	} else {
		d.proposeRequest(msg, cb)
	}
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.Requests != nil && len(msg.Requests) > 0 {
		d.proposeRequest(msg, cb)
	}

	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
