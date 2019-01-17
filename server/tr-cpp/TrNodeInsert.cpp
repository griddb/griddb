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

#include "data_store.h"
#include "internal.h"
#include "transaction_context.h"

/* find an appropriate child to add a rect */
static int32_t TrNode_pick_child(TrNode n, TrRect r) {
	int32_t i, target_child = 0;
	double min_incr = INFINITY, target_vol = 0;

	/* currently, find a child that requires the smallest volume increment */

	for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
		if (n->children[i].nodeOId != UNDEF_OID) {
			TrRect rr = &n->children[i].rect;
			TrRectTag rect = TrRect_surround(r, rr);
			double vol = TrRect_extent_volume(rr);
			double incr = TrRect_extent_volume(&rect) - vol;
			if (incr < min_incr || (incr == min_incr && vol < target_vol)) {
				min_incr = incr;
				target_vol = vol;
				target_child = i;
			}
		}
	}
	return target_child;
}

/* the core part of insertion */
static int32_t TrNode_insert1(TransactionContext &txn,
	ObjectManager &objectManager, OId nOId, TrRect r, OId childOId, OId *nnOId,
	int32_t level) {
	TrChildTag c;
	if (nOId == UNDEF_OID) {
		return 0;
	}
	UpdateBaseObject baseObj(txn.getPartitionId(), objectManager, nOId);
	TrNode n = baseObj.getBaseAddr<TrNode>();

	if (n->level > level) {
		/* the target level is lower; go down recursively */
		int32_t i;
		OId n2OId = UNDEF_OID;

		i = TrNode_pick_child(n, r);

		if (TrNode_insert1(txn, objectManager, n->children[i].nodeOId, r,
				childOId, &n2OId, level)) {
			/* the child node was split; add a new child created by split */
			n->children[i].rect =
				TrNode_surround(txn, objectManager, n->children[i].nodeOId);
			c.nodeOId = n2OId;
			c.rect = TrNode_surround(txn, objectManager, n2OId);
			return TrNode_add_child(txn, objectManager, nOId, &c, nnOId);
		}
		else {
			/* the child node was not split; just fix the rect */
			n->children[i].rect = TrRect_surround(r, &n->children[i].rect);
			return 0;
		}
	}
	else {
		/* the target level is reached; add a new child */
		c.rect = *r;
		c.nodeOId = childOId;
		return TrNode_add_child(txn, objectManager, nOId, &c, nnOId);
	}
}

/* the overview of insertion */
int32_t TrNode_insert0(TransactionContext &txn, ObjectManager &objectManager,
	OId *root, TrRect r, OId childOId, int32_t level) {
	OId nnOId;
	NULL_PTR_CHECK(root);
	NULL_PTR_CHECK(r);
	UNDEF_OID_CHECK(childOId);

	if (TrNode_insert1(txn, objectManager, *root, r, childOId, &nnOId, level)) {
		/* the root was split;
		 * create a new root that includes the old root and the new node
		 */
		OId old_rootOId = *root, new_rootOId = TrNode_new(txn, objectManager);
		BaseObject oldBaseObj(txn.getPartitionId(), objectManager, old_rootOId);
		const TrNode old_root = oldBaseObj.getBaseAddr<const TrNode>();

		UpdateBaseObject baseObj(
			txn.getPartitionId(), objectManager, new_rootOId);
		TrNode new_root = baseObj.getBaseAddr<TrNode>();

		TrChildTag c;

		new_root->level = old_root->level + 1;

		/* the old root is the first child of the new root */
		c.rect = TrNode_surround(txn, objectManager, old_rootOId);
		c.nodeOId = old_rootOId;
		TrNode_add_child(txn, objectManager, new_rootOId, &c, NULL);

		/* the node created by split is the second child of the new root */
		c.rect = TrNode_surround(txn, objectManager, nnOId);
		c.nodeOId = nnOId;
		TrNode_add_child(txn, objectManager, new_rootOId, &c, NULL);

		*root = new_rootOId;
		return 1;
	}
	return 0;
}

/* insert a new data to node */
int32_t TrNode_insert(TransactionContext &txn, ObjectManager &objectManager,
	OId *nodeOId, TrRect r, OId dataOId) {
	if (dataOId == UNDEF_OID) return 0;
	return TrNode_insert0(txn, objectManager, nodeOId, r, dataOId, 0);
}
