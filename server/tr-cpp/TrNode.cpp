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

#include "data_store_v4.h"
#include "internal.h"
#include "transaction_context.h"
#include <stdlib.h>


/* create a node */
OId TrNode_new(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
	OId oId;
	BaseObject allocBaseObj(objectManager, strategy);
	TrNode n = allocBaseObj.allocate<TrNodeTag>(sizeof(TrNodeTag),
		oId, OBJECT_TYPE_RTREE_MAP);
	TrNode_init(n);
	return oId;
}

/* delete node recursively */
void TrNode_destroy(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, OId nodeOId,
	uint64_t &removeNum) {
	UNDEF_OID_CHECK(nodeOId);
	UpdateBaseObject baseObj(objectManager, strategy, nodeOId);
	const TrNode n = baseObj.getBaseAddr<const TrNode>();

	int32_t i;
	if (!TrNode_leaf_p(n)) {
		for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
			if (n->children[i].nodeOId != UNDEF_OID) {
				TrNode_destroy(txn, objectManager, strategy,
					n->children[i].nodeOId, removeNum);
				if (removeNum == 0) {
					break;
				} else {
					n->children[i].nodeOId = UNDEF_OID;
					removeNum--;
				}
			}
		}
	}
	if (removeNum > 0) {
		baseObj.finalize();
	}
}

/* initialize a node */
void TrNode_init(TrNode n) {
	int32_t i;
	NULL_PTR_CHECK(n);
	n->count = 0;
	n->level = -1;
	const int cmax = TR_CHILD_COUNT;
	for (i = 0; i < cmax; i++) n->children[i].nodeOId = UNDEF_OID;
}

/* return a rect that surrounds the rects in children of a node */
TrRectTag TrNode_surround(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, OId nOId) {
	UNDEF_OID_CHECK(nOId);
	BaseObject baseObj(objectManager, strategy, nOId);
	const TrNode n = baseObj.getBaseAddr<const TrNode>();
	int32_t i;
	TrRectTag r = TrRect_null();
	for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
		if (n->children[i].nodeOId != UNDEF_OID) {
			r = TrRect_surround(&r, &(n->children[i].rect));
		}
	}
	return r;
}

/* add a child to a node */
int32_t TrNode_add_child(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	OId nodeOId, TrChild c, OId *nnOId) {
	UNDEF_OID_CHECK(nodeOId);
	NULL_PTR_CHECK(c);
	UpdateBaseObject baseObj(objectManager, strategy, nodeOId);
	TrNode n = baseObj.getBaseAddr<TrNode>();

	if (n->count < TR_CHILD_COUNT_MAX) {
		/* do not split; nn is not used */
		int32_t i;
		for (i = 0; i < TR_CHILD_COUNT_MAX; i++) {
			if (n->children[i].nodeOId == UNDEF_OID) {
				n->children[i] = *c;
				n->count++;
				break;
			}
		}
		return 0;
	}
	else {
		/* split; new node is assigned to *nn */
		NULL_PTR_CHECK(nnOId);
		TrNode_split(txn, objectManager, strategy, nodeOId, c, nnOId);
		return 1;
	}
}

/* remove a child */
void TrNode_delete_child(TrNode n, int32_t i) {
	NULL_PTR_CHECK(n);
	n->children[i].nodeOId = UNDEF_OID;
	n->count--;
}

/* dump a node */
void TrNode_print(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	OId nodeOId, int32_t indent) {
	int32_t i;
	TrChild c;
	UNDEF_OID_CHECK(nodeOId);
	BaseObject baseObj(objectManager, strategy, nodeOId);
	const TrNode n = baseObj.getBaseAddr<const TrNode>();
	TrUtil_print_indent(indent);
	printf("node ");
	printf(TrNode_leaf_p(n) ? "LEAF" : "NODE");
	printf(" level=%d count=%d node=%p\n", n->level, n->count, n);

	if (TrNode_leaf_p(n)) return;

	for (i = 0; i < n->count; i++) {
		TrUtil_print_indent(indent);
		printf("child %d\n", i);
		c = &n->children[i];
		TrRect_print(&(c->rect), indent + 1);
		TrNode_print(txn, objectManager, strategy, c->nodeOId, indent + 1);
	}
}
