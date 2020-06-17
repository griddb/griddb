/*
	Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as
	published by the Free Software Foundation, either version 3 of the
	License, or (at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef SQL_CLUSTER_STAT_H_
#define SQL_CLUSTER_STAT_H_

#include "sql_parser.h"
#include "cluster_common.h"
#include "container_key.h"

class PartitionTable;

template<class T> void randomShuffle(
		util::Vector<T> &ary, int32_t size, util::Random &random) {

	for(int32_t i = 0;i < size; i++) {
		uint32_t j = (uint32_t)(random.nextInt32()) % size;
		T t = ary[i];
		ary[i] = ary[j];
		ary[j] = t;
	}
}

class ClusterStatistics {

	static const int32_t DEFAULT_OWNER_NOT_BALANCE_GAP = 5;

	struct PartitionRangeList {

		struct PartitonRange {

			PartitonRange(
					PartitionId startPId,
					PartitionId endPId,
					int32_t range) :
							startPId_(startPId),
							endPId_(endPId),
							range_(range) {}

			PartitionId startPId_;
			PartitionId endPId_;
			uint32_t range_;
		};
		
		PartitionId set(
				PartitionId startPId, int32_t rangeVal) {

			PartitonRange range(
					startPId, startPId + rangeVal - 1, rangeVal);
			ranges_.push_back(range);
			return (startPId + rangeVal);
		}

		void get(
				int32_t pos,
				PartitionId &startPId,
				PartitionId &endPId) {

			startPId = ranges_[pos].startPId_;
			endPId = ranges_[pos].endPId_;
		}

		PartitionRangeList(
				util::StackAllocator &alloc,
				int32_t partitionNum,
				uint32_t tablePartitioningNum) :
						alloc_(alloc),
						ranges_(alloc) {

			base_ = (partitionNum / tablePartitioningNum);
			mod_ = (partitionNum % tablePartitioningNum);
			uint32_t range = 0;
			PartitionId startPId = 0;

			for (uint32_t pos = 0;
					pos < tablePartitioningNum; pos++) {
				if (pos < mod_) {
					range = base_ + 1;
				}
				else {
					range = base_;
				}
				startPId = set(startPId, range);
			}
		}

		util::StackAllocator &alloc_;
		util::Vector<PartitonRange> ranges_;
		uint32_t base_;
		uint32_t mod_;
		int32_t curentPos_;
	};

public:

	ClusterStatistics(
			util::StackAllocator &alloc,
			PartitionTable *pt,
			int32_t tablePartitioningNum,
			int64_t seed);

	void generateStatistics(
			SyntaxTree::TablePartitionType type,
			PartitionId largeContainerPId, 
			util::Vector<PartitionId> &condenseIdList,
			util::Vector<NodeAffinityNumber> &assignIdList);

private:
	
	void generateHashAssignment(
			util::Vector<PartitionId> &condenseIdList,
			util::Vector<NodeAffinityNumber> &assignIdList);

	void generateIntervalAssignment(
			util::Vector<PartitionId> &condenseIdList,
			util::Vector<NodeAffinityNumber> &assignIdList);

	void generateIntervalHashAssignment(
			util::Vector<PartitionId> &condenseIdList,
			util::Vector<NodeAffinityNumber> &assignIdList);

	util::StackAllocator &alloc_;
	PartitionTable *pt_;
	int32_t tablePartitioningNum_;
	int32_t partitionNum_;
	int32_t nodeNum_;
	int32_t activeNodeNum_;
	util::Vector<uint32_t> assignCountList_;
	util::Vector<NodeId> ownerNodeIdList_;
	util::Vector<util::Vector<PartitionId>> divideList_;
	util::Vector<PartitionId> activeNodeList_;
	util::Vector<PartitionId> activeNodeMap_;
	util::Random random_;
	bool useRandom_;
	int32_t ownerNotBalanceGap_;
	PartitionId largeContainerPId_;
};

#endif
