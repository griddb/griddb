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
/*!
	@file
	@brief RtreeMap Implementation
*/

#ifndef RTREE_MAP_H_
#define RTREE_MAP_H_

#include "util/type.h"
#include "util/container.h"
#include "base_index.h"
#include "data_type.h"
#include "value_processor.h"

#include "TrTree.h"

class BaseContainer;

/*!
 * @brief RtreeMap Class
 */
class RtreeMap : public BaseIndex {
public:
	RtreeMap(TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, BaseContainer *container,
		TreeFuncInfo *funcInfo)
		: BaseIndex(txn, objectManager, strategy, container, funcInfo, MAP_TYPE_SPATIAL),
		  rtreeMapImage_(NULL) {}
	RtreeMap(TransactionContext &txn, ObjectManagerV4 &objectManager, OId oId,
		AllocateStrategy &strategy, BaseContainer *container,
		TreeFuncInfo *funcInfo)
		: BaseIndex(txn, objectManager, oId, strategy, container, funcInfo, MAP_TYPE_SPATIAL) {
		rtreeMapImage_ = reinterpret_cast<RtreeMapImage *>(getBaseAddr());
	}
	~RtreeMap() {}
	struct RtreeCursor {
		uint64_t dummy_;
		RtreeCursor() {
			dummy_ = 0;
		}
	};

private:
	struct RtreeMapImage {
		uint32_t offset_;  
		OId oId_;		   
		uint32_t columnId_;
		ResultSize size_;
		bool isUnique_;
	};
	RtreeMapImage *rtreeMapImage_;

private:
	static int32_t checkCallback(TransactionContext &txn, TrRect r, void *arg);
	static int32_t hitAllCallback(
		TransactionContext &txn, TrRect r, OId hitOId, void *arg);
	static int32_t hitIntersectCallback(
		TransactionContext &txn, TrRect r, OId hitOId, void *arg);
	struct HitIntersectCallbackArg {
		TrRect rect;
		ResultSize limit;
		ResultSize size;
		util::XArray<OId> *oidList;
		OId oneOId;
	};

	static int32_t hitIncludeCallback(
		TransactionContext &txn, TrRect r, OId hitOId, void *arg);
	struct HitIncludeCallbackArg {
		TrRect rect;
		ResultSize limit;
		ResultSize size;
		util::XArray<OId> *oidList;
		TermCondition *conditionList;
		uint32_t conditionSize;
		OId oneOId;
	};

	static int32_t hitDifferentialCallback(
		TransactionContext &txn, TrRect r, OId hitOId, void *arg);
	struct HitDifferentialCallbackArg {
		TrRect rect1, rect2;
		ResultSize limit;
		ResultSize size;
		util::XArray<OId> *oidList;
		TermCondition *conditionList;
		uint32_t conditionSize;
	};

	static int32_t hitQsfIntersectCallback(
		TransactionContext &txn, TrRect r, OId hitOId, void *arg);
	struct HitQsfIntersectCallbackArg {
		TrPv3Key *pkey;
		ResultSize limit;
		ResultSize size;
		util::XArray<OId> *oidList;
		TermCondition *conditionList;
		uint32_t conditionSize;
	};
	static int32_t oIdCmpCallback(
		TransactionContext &txn, TrRect r, OId oId, void *arg);

public:
	static inline bool rectCheckIntersect(TrRect a, TrRect b) {
		return ((a->xmin <= b->xmax) && (b->xmin <= a->xmax) &&
				(a->ymin <= b->ymax) && (b->ymin <= a->ymax) &&
				(a->zmin <= b->zmax) && (b->zmin <= a->zmax));
	}

	static inline bool rectCheckInclude(TrRect outer, TrRect inner) {
		return (outer->xmin <= inner->xmin && outer->xmax >= inner->xmax &&
				outer->ymin <= inner->ymin && outer->ymax >= inner->ymax &&
				outer->zmin <= inner->zmin && outer->zmax >= inner->zmax);
	}

	/*!
	 * @brief Search conditions for RtreeMap
	 *
	 */
	struct SearchContext : BaseIndex::SearchContext {
		struct GeomeryCondition {
			GeomeryCondition() : relation_(GEOMETRY_INTERSECT),
				valid_(false) {
				memset(&pkey_, 0, sizeof(TrPv3Key));
			}
			union {
				TrRectTag rect_[2];
				TrPv3Key pkey_;
			};
			uint8_t relation_;  
			bool valid_;
		};
		TermCondition *getKeyCondition() {
			assert(columnIdList_.size() == 1);
			for (util::Vector<size_t>::iterator itr = keyList_.begin(); 
				itr != keyList_.end(); itr++) {
				if (conditionList_[*itr].opType_ == DSExpression::GEOM_OP) {
					return &(conditionList_[*itr]);
				}
			}
			return NULL;
		}
		SearchContext(util::StackAllocator &alloc, ColumnId columnId)
			: BaseIndex::SearchContext(alloc, columnId) {}
		SearchContext(util::StackAllocator &alloc, util::Vector<ColumnId> &columnIds)
			: BaseIndex::SearchContext(alloc, columnIds) {}
		void copy(util::StackAllocator &alloc, SearchContext &dest) {
			BaseIndex::SearchContext::copy(alloc, dest);
		}
		std::string dump() {
			util::NormalOStringStream strstrm;
			strstrm << BaseIndex::SearchContext::dump();
			return strstrm.str();
		}
	};

	int32_t initialize(TransactionContext &txn, ColumnType ColumnType,
		ColumnId columnId, AllocateStrategy &metaAllocateStrategy,
		bool isUnique = false);
	bool finalize(TransactionContext &txn);
	int32_t clear(TransactionContext &txn);
	uint64_t size();
	int32_t insert(TransactionContext &txn, const void *key, OId oId);
	int32_t remove(TransactionContext &txn, const void *key, OId oId);
	int32_t update(
		TransactionContext &txn, const void *key, OId oId, OId newOId);

	int32_t getAll(
		TransactionContext &txn, ResultSize limit, util::XArray<OId> &idList);
	int32_t search(
		TransactionContext &txn, const void *key, OId &oId);

	void search(TransactionContext &txn, RtreeMap::SearchContext &sc,
		util::XArray<OId> &oidList, OutputOrder outputOrder);

	Geometry *getGeometry(TransactionContext &txn, const void *key);

	int32_t getAll(TransactionContext &txn, ResultSize limit,
		util::XArray<OId> &idList, RtreeCursor &) {
		return getAll(txn, limit, idList);
	}
};

#endif /* RTREE_MAP_H_ */
