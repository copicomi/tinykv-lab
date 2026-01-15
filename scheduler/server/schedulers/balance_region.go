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
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
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

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Select a store to move a region from.
	// We want the store with the largest region size.
	stores := cluster.GetStores()

	// Filter suitable stores
	var suitableStores []*core.StoreInfo
	for _, store := range stores {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}

	if len(suitableStores) <= 1 {
		return nil
	}

	// Sort suitableStores by RegionSize descending
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	for _, sourceStore := range suitableStores {
		var candidateRegions []*core.RegionInfo

		containerFuncs := []func(uint64, func(core.RegionsContainer)){
			cluster.GetPendingRegionsWithLock,
			cluster.GetFollowersWithLock,
			cluster.GetLeadersWithLock,
		}

		for _, fn := range containerFuncs {
			fn(sourceStore.GetID(), func(container core.RegionsContainer) {
				for k := 0; k < balanceRegionRetryLimit; k++ {
					r := container.RandomRegion(nil, nil)
					if r != nil {
						candidateRegions = append(candidateRegions, r)
					}
				}
			})
		}

		if len(candidateRegions) == 0 {
			continue
		}

		// Dedup candidates
		seen := make(map[uint64]struct{})
		var uniqRegions []*core.RegionInfo
		for _, r := range candidateRegions {
			if _, ok := seen[r.GetID()]; !ok {
				seen[r.GetID()] = struct{}{}
				uniqRegions = append(uniqRegions, r)
			}
		}
		candidateRegions = uniqRegions

		for _, region := range candidateRegions {
			if len(region.GetMeta().GetPeers()) < cluster.GetMaxReplicas() {
				continue
			}
			// Find best target store (smallest region size)
			var bestTarget *core.StoreInfo
			var minSize int64 = -1

			for _, target := range suitableStores {
				if target.GetID() == sourceStore.GetID() {
					continue
				}
				if region.GetStorePeer(target.GetID()) != nil {
					continue
				}

				if minSize == -1 || target.GetRegionSize() < minSize {
					minSize = target.GetRegionSize()
					bestTarget = target
				}
			}

			if bestTarget == nil {
				continue
			}

			diff := sourceStore.GetRegionSize() - bestTarget.GetRegionSize()
			// Condition: The move is valuable if diff > 2 * regionSize
			if diff > 2*region.GetApproximateSize() {
				// Allocate new peer
				peer, err := cluster.AllocPeer(bestTarget.GetID())
				if err != nil {
					return nil
				}

				op, err := operator.CreateMovePeerOperator(
					"balance-region",
					cluster,
					region,
					operator.OpBalance,
					sourceStore.GetID(),
					bestTarget.GetID(),
					peer.GetId(),
				)
				if err != nil {
					continue
				}
				return op
			}
		}
	}

	return nil
}
