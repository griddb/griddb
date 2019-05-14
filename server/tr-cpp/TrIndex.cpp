/*
	Copyright (c) 2011 TOSHIBA CORPORATION

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
	@brief TR: R-Tree implementation
*/

#include "util/type.h"
#include "data_store.h"
#include "internal.h"
#include "object_manager.h"
#include "transaction_context.h"
#include <stdlib.h>

/* create an index */
OId TrIndex_new(TransactionContext &txn, ObjectManager &objectManager) {
	OId oId;

	BaseObject allocBaseObj(txn.getPartitionId(), objectManager);
	TrIndex idx = allocBaseObj.allocate<TrIndexTag>(sizeof(TrIndexTag),
		AllocateStrategy(ALLOCATE_NO_EXPIRE_MAP), oId, OBJECT_TYPE_RTREE_MAP);

	idx->rootNodeOId = TrNode_new(txn, objectManager);
	UpdateBaseObject baseObj(
		txn.getPartitionId(), objectManager, idx->rootNodeOId);
	TrNode n = baseObj.getBaseAddr<TrNode>();

	n->level = 0;
	idx->count = 0;
	return oId;
}

/* destroy an index */
void TrIndex_destroy(
	TransactionContext &txn, ObjectManager &objectManager, OId idxOId, uint64_t &removeNum) {
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	UpdateBaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	const TrIndex idx = baseObj.getBaseAddr<const TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}
	if (idx->rootNodeOId != UNDEF_OID) {
		TrNode_destroy(txn, objectManager, idx->rootNodeOId, removeNum);
	}
	if (removeNum > 0) {
		baseObj.finalize();
	}
}

/* return the entry size of an index */
int32_t TrIndex_size(
	TransactionContext &txn, ObjectManager &objectManager, OId idxOId) {
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	BaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	const TrIndex idx = baseObj.getBaseAddr<const TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}
	return idx->count;
}

/* enumerate all entries */
void TrIndex_all(TransactionContext &txn, ObjectManager &objectManager,
	OId idxOId, TrHitCallback cb, void *cbarg) {
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	if (cb == NULL || cbarg == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE,
			"HitCallback or arg is undefined.");
	}

	BaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	const TrIndex idx = baseObj.getBaseAddr<const TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}
	TrNode_all(txn, objectManager, idx->rootNodeOId, cb, cbarg);
}
void TrIndex_dump(
	TransactionContext &txn, ObjectManager &objectManager, OId idxOId) {
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}

	BaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	const TrIndex idx = baseObj.getBaseAddr<const TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}
	TrNode_all_dump(txn, objectManager, idx->rootNodeOId);
}

/* serach an entry by using user-defined collision detection */
void TrIndex_search(TransactionContext &txn, ObjectManager &objectManager,
	OId idxOId, TrCheckCallback ccb, void *ccbarg, TrHitCallback hcb,
	void *hcbarg) {
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	if (ccb == NULL || ccbarg == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE,
			"CheckCallback or arg is undefined.");
	}
	if (hcb == NULL || hcbarg == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE,
			"HitCallback or arg is undefined.");
	}

	BaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	const TrIndex idx = baseObj.getBaseAddr<const TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}

	TrNode_search(
		txn, objectManager, idx->rootNodeOId, ccb, ccbarg, hcb, hcbarg);
}

/* search all entries that overlap with the given rect */
void TrIndex_search_rect(TransactionContext &txn, ObjectManager &objectManager,
	OId idxOId, TrRect r, TrHitCallback cb, void *cbarg) {
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	if (cb == NULL || cbarg == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE,
			"HitCallback or arg is undefined.");
	}
	if (r == NULL || r->xmax < r->xmin || r->ymax < r->ymin ||
		r->zmax < r->zmin) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "Invalid rect is specified");
	}

	BaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	const TrIndex idx = baseObj.getBaseAddr<const TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}
	TrNode_search_rect(txn, objectManager, idx->rootNodeOId, r, cb, cbarg);
}

void TrIndex_search_quad(TransactionContext &txn, ObjectManager &objectManager,
	OId idxOId, TrPv3Key *qkey, TrHitCallback cb, void *cbarg) {
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	if (cb == NULL || cbarg == NULL || qkey == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE,
			"HitCallback or arg is undefined.");
	}
	BaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	const TrIndex idx = baseObj.getBaseAddr<const TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}
	TrNode_search_quad(txn, objectManager, idx->rootNodeOId, qkey, cb, cbarg);
}

/* insert a new entry to an index */
int32_t TrIndex_insert(TransactionContext &txn, ObjectManager &objectManager,
	OId idxOId, TrRect r, OId dataOId) {
	int32_t ret;
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	if (r == NULL || r->xmax < r->xmin || r->ymax < r->ymin ||
		r->zmax < r->zmin) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "Invalid rect is specified");
	}
	if (dataOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE,
			"Invalid dataOId is specified");
	}

	UpdateBaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	TrIndex idx = baseObj.getBaseAddr<TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}
	ret = TrNode_insert(txn, objectManager, &idx->rootNodeOId, r, dataOId);
	idx->count++;
	return ret;
}

/* delete an entry from an index */
int32_t TrIndex_delete(TransactionContext &txn, ObjectManager &objectManager,
	OId idxOId, TrRect r, void *data) {
	int32_t ret;
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	if (r == NULL || r->xmax < r->xmin || r->ymax < r->ymin ||
		r->zmax < r->zmin) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "Invalid rect is specified");
	}
	if (data == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "Invalid data");
	}

	UpdateBaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	TrIndex idx = baseObj.getBaseAddr<TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}

	ret = TrNode_delete(txn, objectManager, &idx->rootNodeOId, r, data);
	if (ret) {
		--idx->count;
	}
	return ret;
}

int32_t TrIndex_delete_cmp(TransactionContext &txn,
	ObjectManager &objectManager, OId idxOId, TrRect r, TrDataCmpCallback dccb,
	void *dccbarg) {
	int32_t ret;
	if (idxOId == UNDEF_OID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "idxOId is undefined.");
	}
	if (r == NULL || r->xmax < r->xmin || r->ymax < r->ymin ||
		r->zmax < r->zmin) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE, "Invalid rect is specified");
	}
	if (dccb == NULL || dccbarg == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE,
			"DeleteCallback or arg is undefined.");
	}

	UpdateBaseObject baseObj(txn.getPartitionId(), objectManager, idxOId);
	TrIndex idx = baseObj.getBaseAddr<TrIndex>();
	if (idx == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_DATA_ERROR, "Cannot obtain index data.");
	}

	ret = TrNode_delete_cmp(
		txn, objectManager, &idx->rootNodeOId, r, dccb, dccbarg);
	if (ret) --idx->count;
	return ret;
}
