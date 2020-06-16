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
#include "sql_cluster_stat.h"
#include "partition_table.h"

ClusterStatistics::ClusterStatistics(
		util::StackAllocator &alloc,
		PartitionTable *pt,
		int32_t tablePartitioningNum,
		int64_t seed) :
				alloc_(alloc),
				pt_(pt),
				tablePartitioningNum_(tablePartitioningNum), 
				partitionNum_(pt->getPartitionNum()),
				assignCountList_(alloc),
				ownerNodeIdList_(alloc),
				divideList_(alloc),
				activeNodeList_(alloc),
				activeNodeMap_(alloc), 
				random_(seed),
				useRandom_(false),
				ownerNotBalanceGap_(DEFAULT_OWNER_NOT_BALANCE_GAP),
				largeContainerPId_(UNDEF_PARTITIONID) {

	if (pt == NULL || partitionNum_ == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INTERNAL,
				"Invalid cluster statistics parameter");
	}

	ownerNodeIdList_.assign(
			pt_->getPartitionNum(), UNDEF_NODEID);

	nodeNum_ = pt_->getNodeNum();
	pt_->getLiveNodeIdList(activeNodeList_);
	activeNodeNum_
			= static_cast<int32_t>(activeNodeList_.size());

	if (nodeNum_ == 0 || activeNodeNum_ == 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
				"Active node is not found");
	}

	assignCountList_.assign(activeNodeNum_, 0);

	NodeId ownerNodeId;
	int32_t nodePos;
	activeNodeMap_.assign(nodeNum_, -1);
	assert(activeNodeNum_ != 0);
	
	for (size_t i = 0;i < activeNodeList_.size(); i++) {
		NodeId nodeId = activeNodeList_[i];
		if (nodeId >= nodeNum_) {
			continue;
		}
		activeNodeMap_[nodeId] = static_cast<NodeId>(i);
	}
	
	for (nodePos = 0;nodePos < activeNodeNum_;nodePos++) {
		util::Vector<PartitionId> tmpIdList(alloc);
		divideList_.push_back(tmpIdList);
	}
	
	for (int32_t pId = 0; pId < partitionNum_; pId++) {
		ownerNodeId = pt_->getNewSQLOwner(pId);
		ownerNodeIdList_[pId] = ownerNodeId;
		if (ownerNodeId == UNDEF_NODEID || ownerNodeId >= nodeNum_) {
			useRandom_ = true;
			continue;
		}
		nodePos = activeNodeMap_[ownerNodeId];
		if (nodePos == -1) {
			useRandom_ = true;
			continue;
		}
		assert(nodePos != -1);
		divideList_[nodePos].push_back(pId);
	}
	
	int32_t max = INT32_MIN;
	int32_t min = INT32_MAX;
	int32_t count = 0;
	
	for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
		count = static_cast<int32_t>(divideList_.size());
		if (count > max) {
			max = count;
		}
		if (count < min) {
			min = count;
		}
	}
	
	if ((max - min) > ownerNotBalanceGap_) {
		useRandom_ = true;
	}
}
	
void ClusterStatistics::generateStatistics(
		SyntaxTree::TablePartitionType type,
		PartitionId largeContainerPId, 
		util::Vector<PartitionId> &condenseIdList,
		util::Vector<NodeAffinityNumber> &assignIdList) {
	
	largeContainerPId_ = largeContainerPId;
	
	switch (type) {
	
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
			generateHashAssignment(
					condenseIdList, assignIdList);
		break;
	
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
			generateIntervalAssignment(
					condenseIdList, assignIdList);
		break;

		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
			generateIntervalHashAssignment(
					condenseIdList, assignIdList);
		break;

		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_UNSUPPORTED_PARTITINION_INFO,
					"Unsupported table partitioning type");
			break;
	}
}

void ClusterStatistics::generateHashAssignment(
		util::Vector<PartitionId> &condenseIdList,
		util::Vector<NodeAffinityNumber> &assignIdList) {

	int32_t currentTablePartitioningNum = tablePartitioningNum_;
	bool isOverPartitionNum = false;
	assignIdList.assign(tablePartitioningNum_ + 1, 0);
	
	if (tablePartitioningNum_ >= partitionNum_) {
		currentTablePartitioningNum = partitionNum_;
		isOverPartitionNum = true;
		condenseIdList.assign(partitionNum_, 0);
	}
	else {
		condenseIdList.assign(tablePartitioningNum_, 0);
	}
	
	PartitionRangeList ranges(
			alloc_,
			partitionNum_, 
			currentTablePartitioningNum);

	PartitionId startPId, endPId;
	util::Vector<int32_t> rangeNoList(alloc_);
	rangeNoList.assign(currentTablePartitioningNum, 0);

	int32_t rangePos;
	for (rangePos = 0;
			rangePos < currentTablePartitioningNum; rangePos++) {
		rangeNoList[rangePos] = rangePos;
	}

	randomShuffle(
			rangeNoList, currentTablePartitioningNum, random_);

	PartitionId targetPId;
	int32_t nodePos  = 0;
	for (rangePos = 0;
			rangePos < currentTablePartitioningNum; rangePos++) {

		ranges.get(rangeNoList[rangePos], startPId, endPId);
		uint32_t minVal = UINT32_MAX;
		PartitionId target;
		bool assigned = false;

		util::Vector<int32_t> candList(alloc_);
		if (!useRandom_) {
			for (target = startPId; target <= endPId; target++) {
				nodePos = activeNodeMap_[ownerNodeIdList_[target]];				
				if (nodePos == -1) continue;

				if (minVal > assignCountList_[nodePos]) {
					minVal = assignCountList_[nodePos];
				}
			}

			if (minVal != UINT32_MAX) {
				for (target = startPId; target <= endPId; target++) {
					nodePos = activeNodeMap_[ownerNodeIdList_[target]];
					if (nodePos == -1) continue;
					if (minVal == assignCountList_[nodePos]) {
						candList.push_back(target);
						assigned = true;
					}
				}
			}
		}

		if (!assigned) {
			int32_t rangeSize = endPId - startPId;
			int32_t randomPos = random_.nextInt32(rangeSize);
			targetPId = startPId + randomPos;
		}
		else {
			int32_t randomPos = random_.nextInt32(
					static_cast<int32_t>(candList.size()));
			targetPId = candList[randomPos];
		}

		if (ownerNodeIdList_[targetPId] != UNDEF_NODEID)  {
			nodePos = activeNodeMap_[ownerNodeIdList_[targetPId]];
			if (nodePos != -1) {
				assignCountList_[nodePos]++;
			}
		}
		condenseIdList[rangePos] = targetPId;
	}

	size_t assignPos = 0;
	assignIdList[0] = largeContainerPId_;
	assignPos++;

	if (!isOverPartitionNum) {
		for (size_t pos = 0; pos < condenseIdList.size(); pos++) {
			assignIdList[assignPos++] = (condenseIdList[pos] + partitionNum_);
		}
	}
	else {
		for (rangePos = 0; rangePos < tablePartitioningNum_; rangePos++) {
			assignIdList[assignPos++] =
					((rangePos / currentTablePartitioningNum) * partitionNum_
							+ condenseIdList[rangePos % currentTablePartitioningNum]
							+ partitionNum_);
		}
	}
}

void ClusterStatistics::generateIntervalAssignment(
		util::Vector<PartitionId> &condenseIdList,
		util::Vector<NodeAffinityNumber> &assignIdList) {

	assignIdList.push_back(largeContainerPId_);
	condenseIdList.assign(partitionNum_, 0);
	int32_t nodePos;
	
	if (useRandom_) {
		for(int32_t pId = 0;pId < partitionNum_; pId++) {
			condenseIdList[pId] = pId;
		}
		randomShuffle(condenseIdList, partitionNum_, random_);
		return;
	}
	
	for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
		if (divideList_[nodePos].size() != 0) {
			randomShuffle(
					divideList_[nodePos],
					static_cast<int32_t>(divideList_[nodePos].size()),
					random_);
		}
	}

	uint32_t minVal = UINT32_MAX;
	util::Vector<int32_t> candList(alloc_);
	int32_t emptyCount = 0;
	int32_t assignedCount = 0;
	int32_t targetNodePos;
	
	while(1) {
		minVal = UINT32_MAX;
		emptyCount = 0;
		candList.clear();
		for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
			if (divideList_[nodePos].size() == 0) {
				emptyCount++;
				continue;
			}
			if (minVal > assignCountList_[nodePos]) {
				minVal = assignCountList_[nodePos];
			}
		}
		if (emptyCount == activeNodeNum_) {
			break;
		}
		for (nodePos = 0; nodePos < activeNodeNum_; nodePos++) {
			if (divideList_[nodePos].size() == 0) continue;
			if (minVal == assignCountList_[nodePos]) {
				candList.push_back(nodePos);
			}
		}
		targetNodePos = candList[random_.nextInt32(
				static_cast<int32_t>(candList.size()))];
		assert(targetNodePos != -1);

		condenseIdList[assignedCount]
			= divideList_[targetNodePos].back();
		assignCountList_[targetNodePos]++;
		assignedCount++;
		divideList_[targetNodePos].pop_back();
	}
}

void ClusterStatistics::generateIntervalHashAssignment(
		util::Vector<PartitionId> &condenseIdList,
		util::Vector<NodeAffinityNumber> &assignIdList) {

	generateIntervalAssignment(condenseIdList, assignIdList);

	for(int32_t pId = 0;pId < partitionNum_; pId++) {
		condenseIdList[pId] *= tablePartitioningNum_;
	}
}