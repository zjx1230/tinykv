// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type SortStoreInfo []*core.StoreInfo

func (s SortStoreInfo) Len() int           { return len(s) }
func (s SortStoreInfo) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SortStoreInfo) Less(i, j int) bool { return s[j].GetRegionSize() < s[i].GetRegionSize() } // 从大到小

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	suitableStoreInfos := make(SortStoreInfo, 0)
	for _, storeInfo := range cluster.GetStores() {
		if storeInfo.IsUp() && storeInfo.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStoreInfos = append(suitableStoreInfos, storeInfo)
		}
	}

	if len(suitableStoreInfos) < 2 {
		return nil
	}

	sort.Sort(suitableStoreInfos)
	var selectedRegionInfo *core.RegionInfo
	chooseSrcIndex := -1
	for i, storeInfo := range suitableStoreInfos {
		cluster.GetPendingRegionsWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
			selectedRegionInfo = container.RandomRegion(nil, nil)
		})

		if selectedRegionInfo != nil {
			chooseSrcIndex = i
			break
		}

		cluster.GetFollowersWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
			selectedRegionInfo = container.RandomRegion(nil, nil)
		})

		if selectedRegionInfo != nil {
			chooseSrcIndex = i
			break
		}

		cluster.GetLeadersWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
			selectedRegionInfo = container.RandomRegion(nil, nil)
		})

		if selectedRegionInfo != nil {
			chooseSrcIndex = i
			break
		}
	}

	if selectedRegionInfo == nil {
		return nil
	}

	if len(selectedRegionInfo.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}

	if chooseSrcIndex == len(suitableStoreInfos)-1 { // smallest
		return nil
	}

	chooseTargetIndex := -1
	OriginalStoreIds := selectedRegionInfo.GetStoreIds()
	for i := len(suitableStoreInfos) - 1; i > chooseSrcIndex; i-- {
		if _, ok := OriginalStoreIds[suitableStoreInfos[i].GetID()]; !ok {
			chooseTargetIndex = i
			break
		}
	}

	if chooseTargetIndex == -1 {
		return nil
	}

	srcStore := suitableStoreInfos[chooseSrcIndex]
	targetStore := suitableStoreInfos[chooseTargetIndex]
	if srcStore.GetRegionSize()-targetStore.GetRegionSize() > 2*selectedRegionInfo.GetApproximateSize() {
		newPeer, err := cluster.AllocPeer(targetStore.GetID())
		if err != nil {
			log.Errorf("cluster.AllocPeer err: %v\n", err)
			return nil
		}
		desc := fmt.Sprintf("move the region: %d, from src store id: %d to the target store id: %d\n", selectedRegionInfo.GetID(), srcStore.GetID(), targetStore.GetID())
		op, err := operator.CreateMovePeerOperator(desc, cluster, selectedRegionInfo, operator.OpBalance, srcStore.GetID(), targetStore.GetID(), newPeer.Id)
		if err != nil {
			log.Errorf("operator.CreateMovePeerOperator err: %v\n", err)
			return nil
		}
		return op
	}
	return nil
}
