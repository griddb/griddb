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

#include "util/allocator.h"
#include "data_store.h"
#include "internal.h"
#include "transaction_context.h"
#include <stdlib.h>

/* a node list for remembering reinsertion */
typedef struct _TrNodeListTag {
	struct _TrNodeListTag *next;
	OId nodeOId;
} * TrNodeList;

static TrNodeList TrList_new(TransactionContext &txn) {
	TrNodeList l;
	l = static_cast<TrNodeList>(txn.getDefaultAllocator().allocate(sizeof(*l)));
	return l;
}

static void TrList_free(TransactionContext &txn, TrNodeList l) {
	txn.getDefaultAllocator().deallocate(l);
}

static TrNodeList TrList_add(TransactionContext &txn, TrNodeList l, OId nOId) {
	TrNodeList nl = TrList_new(txn);
	nl->nodeOId = nOId;
	nl->next = l;
	return nl;
}

/* the core part of deletion */
static int32_t TrNode_delete1(TransactionContext &txn,
	ObjectManager &objectManager, OId nOId, TrRect r, TrNodeList *l,
	TrDataCmpCallback dccb, void *dccbarg, int &num) {
	int ret = 0;
	if (nOId == UNDEF_OID) {
		return 0;
	}
	UpdateBaseObject baseObj(txn.getPartitionId(), objectManager, nOId);
	TrNode n = baseObj.getBaseAddr<TrNode>();
	if (n == NULL) {
		return 0;
	}

	int32_t i;
	if (TrNode_leaf_p(n)) {
		for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
			if (n->children[i].nodeOId != UNDEF_OID &&
				memcmp(&n->children[i].rect, r, sizeof(TrRectTag)) == 0 &&
				dccb(txn, &n->children[i].rect, n->children[i].nodeOId,
					dccbarg)) {
				TrNode_delete_child(n, i);
				++num;
				ret = 1;
				break;
			}
		}
	}
	else {
		for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
			if (n->children[i].nodeOId != UNDEF_OID &&
				TrRect_overlap_p(r, &n->children[i].rect)) {
				if (!TrNode_delete1(txn, objectManager, n->children[i].nodeOId,
						r, l, dccb, dccbarg, num)) {
					BaseObject baseObj(txn.getPartitionId(), objectManager,
						n->children[i].nodeOId);
					const TrNode cNode = baseObj.getBaseAddr<const TrNode>();
					if (cNode == NULL) {
						continue;
					}
					if (cNode->count < TR_CHILD_COUNT_MIN) {
						/* the children is too few;
						 * once remove the node, and will insert it later
						 */
						*l = TrList_add(txn, *l, n->children[i].nodeOId);
						TrNode_delete_child(n, i);
						++num;
					}
					else {
						/* the node has enough children; just fix the rect */
						n->children[i].rect = TrNode_surround(
							txn, objectManager, n->children[i].nodeOId);
					}
					ret = 1;
				}
			}
		}
	}
	int32_t currPos = 0;
	for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
		if (n->children[i].nodeOId != UNDEF_OID) {
			n->children[currPos] = n->children[i];
			if (i != currPos) {
				n->children[i].nodeOId = UNDEF_OID;
			}
			currPos++;
		}
	}
	return ret;
}

/* reinsert lost children */
static void TrNode_reinsert(TransactionContext &txn,
	ObjectManager &objectManager, OId *rootOId, TrNodeList l, int &num) {
	int32_t i;
	TrNodeList list;
	OId nodeOId = UNDEF_OID;
	while (l) {
		num--;
		/* fetch a node */
		list = l;
		nodeOId = l->nodeOId;

		UpdateBaseObject baseObj(txn.getPartitionId(), objectManager, nodeOId);
		const TrNode node = baseObj.getBaseAddr<const TrNode>();

		l = l->next;
		TrList_free(txn, list);

		/* reinsert the node */
		for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
			TrChild c = &node->children[i];
			if (c->nodeOId != UNDEF_OID) {
				TrNode_insert0(txn, objectManager, rootOId, &c->rect,
					c->nodeOId, node->level);
			}
		}
		baseObj.finalize();
	}
}

/* the overview of deletion */
static int32_t TrNode_delete0(TransactionContext &txn,
	ObjectManager &objectManager, OId *rootOId, TrRect r,
	TrDataCmpCallback dccb, void *dccbarg, int &num) {
	NULL_PTR_CHECK(rootOId);
	UNDEF_OID_CHECK(*rootOId);

	UpdateBaseObject baseObj(txn.getPartitionId(), objectManager, *rootOId);
	TrNode root = baseObj.getBaseAddr<TrNode>();
	TrNodeList list = NULL;
	TrNode_delete1(txn, objectManager, *rootOId, r, &list, dccb, dccbarg, num);
	if (num >= 1) {
		/* the child was found and deleted */
		int32_t i;
		OId nodeOId = UNDEF_OID;

		/* process reinsertion list */
		TrNode_reinsert(txn, objectManager, rootOId, list, num);

		assert(num >= 0);

		/* if the root has only one child, make it a new root */
		if (root->count == 1 && !TrNode_leaf_p(root)) {
			for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
				nodeOId = root->children[i].nodeOId;
				if (nodeOId != UNDEF_OID) break;
			}
			baseObj.finalize();
			*rootOId = nodeOId;
		}
		return 1;
	}
	return 0;
}

/* delete an entry that overlaps the given rect */
/* with comparison function for data value */
int32_t TrNode_delete_cmp(TransactionContext &txn, ObjectManager &objectManager,
	OId *rootOId, TrRect r, TrDataCmpCallback dccb, void *dccbarg) {
	int num = 0;
	NULL_PTR_CHECK(rootOId);
	UNDEF_OID_CHECK(*rootOId);
	NULL_PTR_CHECK(r);
	NULL_PTR_CHECK(dccb);
	TrNode_delete0(txn, objectManager, rootOId, r, dccb, dccbarg, num);
	return num;
}

static int32_t tr_cmp(TransactionContext &, TrRect, OId data1, void *data2) {
	OId *p = reinterpret_cast<OId *>(data2);
	return data1 == *p;
}

/* delete an entry that overlaps the given rect */
int32_t TrNode_delete(TransactionContext &txn, ObjectManager &objectManager,
	OId *rootOId, TrRect r, void *data) {
	NULL_PTR_CHECK(rootOId);
	UNDEF_OID_CHECK(*rootOId);
	NULL_PTR_CHECK(r);
	NULL_PTR_CHECK(data);
	return TrNode_delete_cmp(txn, objectManager, rootOId, r, tr_cmp, data);
}
