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
#include "object_manager.h"
#include "transaction_context.h"

/* serach an entry by using user-defined collision detection */
void TrNode_search(TransactionContext &txn, ObjectManager &objectManager,
	OId nodeOId, TrCheckCallback ccb, void *ccbarg, TrHitCallback hcb,
	void *hcbarg)
#if 1
{
	UNDEF_OID_CHECK(nodeOId);
	NULL_PTR_CHECK(ccb);
	NULL_PTR_CHECK(hcb);

	BaseObject baseObj(txn.getPartitionId(), objectManager, nodeOId);
	const TrNode n = baseObj.getBaseAddr<const TrNode>();

	int32_t i, j;
	TrChildTag children[TR_CHILD_COUNT_MAX];

	for (i = j = 0; i < TR_CHILD_COUNT_MAX; i++) {
		TrChild b = &n->children[i];
		if (b->nodeOId != UNDEF_OID && ccb(txn, &b->rect, ccbarg)) {
			children[j++] = n->children[i];
		}
	}

	if (TrNode_leaf_p(n)) {
		for (i = 0; i < j; i++) {
			TrChild b = &children[i];
			if (!hcb(txn, &b->rect, b->nodeOId, hcbarg)) return;
		}
	}
	else {
		for (i = 0; i < j; i++) {
			TrChild b = &children[i];
			TrNode_search(
				txn, objectManager, b->nodeOId, ccb, ccbarg, hcb, hcbarg);
		}
	}
}
#else
{
	BaseObject baseObj(txn.getPartitionId(), txn.getObjectManager(), nodeOId);
	const TrNode n = baseObj.getBaseAddr<const TrNode>();
	int32_t i, j;
	TrChildTag children[TR_CHILD_COUNT_MAX];

	for (i = j = 0; i < TR_CHILD_COUNT_MAX; i++) {
		TrChild b = &n->children[i];
		if (b->nodeOId && ccb(&b->rect, ccbarg)) {
			children[j++] = n->children[i];
		}
	}

	if (TrNode_leaf_p(n)) {
		for (i = 0; i < j; i++) {
			TrChild b = &children[i];
			if (!hcb(&b->rect, b->nodeOId, hcbarg)) return;
		}
	}
	else {
		for (i = 0; i < j; i++) {
			TrChild b = &children[i];
			TrNode_search(
				txn, objectManager, b->nodeOId, ccb, ccbarg, hcb, hcbarg);
		}
	}
}
#endif

void TrNode_dump(TransactionContext &txn, ObjectManager &objectManager,
	OId nodeOId, int32_t depth) {
	UNDEF_OID_CHECK(nodeOId);

	BaseObject baseObj(txn.getPartitionId(), objectManager, nodeOId);
	const TrNode n = baseObj.getBaseAddr<const TrNode>();

	int32_t i, j;
	TrChildTag children[TR_CHILD_COUNT_MAX];

	for (i = 0; i < depth; i++) {
		std::cout << "  ";
	}
	if (TrNode_leaf_p(n)) {
		std::cout << "(" << depth << ")RLeafNode[" << nodeOId << "]=[";
	}
	else {
		std::cout << "(" << depth << ")RInterNode[" << nodeOId << "]=[";
	}
	for (i = j = 0; i < TR_CHILD_COUNT_MAX; i++) {
		if (i != 0) {
			std::cout << ", ";
		}

		TrChild b = &n->children[i];
		if (b->nodeOId != UNDEF_OID) {
			children[j++] = n->children[i];
			std::cout << n->children[i].nodeOId << "("
					  << n->children[i].rect.xmin << ","
					  << n->children[i].rect.xmax << ","
					  << n->children[i].rect.ymin << ","
					  << n->children[i].rect.ymax << ","
					  << n->children[i].rect.zmin << ","
					  << n->children[i].rect.zmax << ","
					  << ")";
		}
		else {
			std::cout << "*";
		}
	}
	std::cout << "]" << std::endl;

	if (!TrNode_leaf_p(n)) {
		for (i = 0; i < j; i++) {
			TrChild b = &children[i];
			TrNode_dump(txn, objectManager, b->nodeOId, depth + 1);
		}
	}
}

static int32_t each_check_cb(TransactionContext &txn, TrRect r, void *ccbarg) {
	(void)txn;
	(void)r;
	(void)ccbarg;
	return 1;
}

/* enumerate all entries */
void TrNode_all(TransactionContext &txn, ObjectManager &objectManager, OId nOId,
	TrHitCallback hcb, void *hcbarg) {
	TrNode_search(txn, objectManager, nOId, each_check_cb, 0, hcb, hcbarg);
}
void TrNode_all_dump(
	TransactionContext &txn, ObjectManager &objectManager, OId nOId) {
	std::cout << "=========Rtree dump start========" << std::endl;
	TrNode_dump(txn, objectManager, nOId, 0);
	std::cout << "=========Rtree dump end========" << std::endl;
}

/* Callback for overlap */
int32_t TrRect_overlap_p_cb(TransactionContext &, TrRect r, void *ccbarg) {
	return TrRect_overlap_p(r, (TrRect)ccbarg);
}

/* search all entries that overlap with the given rect */
void TrNode_search_rect(TransactionContext &txn, ObjectManager &objectManager,
	OId nOId, TrRect r, TrHitCallback hcb, void *hcbarg) {
	NULL_PTR_CHECK(r);
	if (r->xmin > r->xmax) {
		return;
	}
	TrNode_search(txn, objectManager, nOId,
		(TrCheckCallback)TrRect_overlap_p_cb, r, hcb, hcbarg);
}

static int32_t qkey_check_cb(TransactionContext &txn, TrRect r, void *ccbarg) {
	(void)txn;
	TrPv3Key *qkey = (TrPv3Key *)ccbarg;
	TrPv3Box box;

	box.p0[0] = r->xmin;
	box.p0[1] = r->ymin;
	box.p0[2] = r->zmin;
	box.p1[0] = r->xmax - r->xmin;
	box.p1[1] = r->ymax - r->ymin;
	box.p1[2] = r->zmax - r->zmin;

	return TrPv3Test2(&box, qkey);
}

void TrNode_search_quad(TransactionContext &txn, ObjectManager &objectManager,
	OId nOId, TrPv3Key *qkey, TrHitCallback hcb, void *hcbarg) {
	NULL_PTR_CHECK(qkey);
	TrNode_search(txn, objectManager, nOId, qkey_check_cb, qkey, hcb, hcbarg);
}
