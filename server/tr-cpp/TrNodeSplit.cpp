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

#include "internal.h"
#include "object_manager_v4.h"
#include "schema.h"
#include "transaction_context.h"
#include <stdlib.h>
#include <string.h>

#define TOTAL (TR_CHILD_COUNT + 1)

typedef enum TrGroupTag { undecided = -1, group1, group2 } TrGroup;

/* information for split */
typedef struct _TrSplitTag {
	/* per-child information */
	TrChildTag nodes[TR_CHILD_COUNT + 1]; /* temporarily buffer */
	TrGroup partition[TR_CHILD_COUNT + 1];
	/* which group each node beint32_ts to */

	/* per-group information */
	int32_t group_count[2];  /* size of the group */
	TrRectTag group_rect[2]; /* rects covered by the group */
	double group_area[2];	/* area of the rect */
} TrSplitTag, *TrSplit;

/* split start */
static void TrSplit_initialize(TrSplit s, TrNode n, TrChild b) {
	int32_t i;

	/* copy all child nodes to buffer */
	memcpy(s->nodes, n->children, sizeof(TrChildTag) * (TOTAL - 1));
	s->nodes[TOTAL - 1] = *b;
	for (i = 0; i < TOTAL; i++) s->partition[i] = undecided;

	/* delete all children */
	TrNode_init(n);

	/* initialize two groups as empty */
	s->group_count[0] = s->group_count[1] = 0;
	s->group_rect[0] = s->group_rect[1] = TrRect_null();
	s->group_area[0] = s->group_area[1] = 0.0;
}

/* split end */
static void TrSplit_finalize(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, TrSplit s, OId n1OId, OId n2OId) {
	int32_t i;

	/* load buffered children to a node according to partition result */
	for (i = 0; i < TOTAL; i++) {
		OId nodeOId = s->partition[i] == group1 ? n1OId : n2OId;
		if (nodeOId != UNDEF_OID) {
			TrNode_add_child(txn, objectManager, strategy, nodeOId, &s->nodes[i], NULL);
		}
	}
}

/* set a node as the group */
static void TrDecideGroup(TrSplit s, int32_t i, TrGroup group) {
	s->partition[i] = group;

	s->group_count[group]++;
	s->group_rect[group] =
		TrRect_surround(&s->nodes[i].rect, &s->group_rect[group]);
	s->group_area[group] = TrRect_extent_volume(&s->group_rect[group]);
}

/* select the first two nodes as a seed of each group */
static void TrPickFirstSeeds(TrSplit s) {
	int32_t i, j, seed1 = 0, seed2 = 0;
	double max_incr, incr, area[TR_CHILD_COUNT + 1];

	/* pre-calculate area of each child */
	for (i = 0; i < TOTAL; i++) {
		if (s->nodes[i].nodeOId != UNDEF_OID) {
			area[i] = TrRect_extent_volume(&s->nodes[i].rect);
		}
	}

	/* select the two in quadratic approach */
	max_incr = -INFINITY;
	for (i = 0; i < TOTAL - 1; i++) {
		for (j = i + 1; j < TOTAL; j++) {
			if (s->nodes[i].nodeOId != UNDEF_OID &&
				s->nodes[j].nodeOId != UNDEF_OID) {
				TrRectTag one_rect;
				one_rect =
					TrRect_surround(&s->nodes[i].rect, &s->nodes[j].rect);
				incr = TrRect_extent_volume(&one_rect) - area[i] - area[j];
				if (incr > max_incr) {
					max_incr = incr;
					seed1 = i;
					seed2 = j;
				}
			}
		}
	}

	/* set seed1 as group1, and seed2 as group2 */
	TrDecideGroup(s, seed1, group1);
	TrDecideGroup(s, seed2, group2);
}

static double TrSelectGroup(TrSplit s, TrRect r, TrGroup *group) {
	TrRectTag rect1 = TrRect_surround(r, &s->group_rect[0]);
	TrRectTag rect2 = TrRect_surround(r, &s->group_rect[1]);
	double incr1 = TrRect_extent_volume(&rect1) - s->group_area[0];
	double incr2 = TrRect_extent_volume(&rect2) - s->group_area[1];
	double cost = incr2 - incr1;

	*group = cost >= 0 ? group1 : group2;
	if (cost < 0) cost = -cost;

	return cost;
}

/* distribute all pending nodes to two groups */
static void TrCategorize(TrSplit s) {
	int32_t i, target_node = 0;
	TrGroup group, target_group = group1;
	double cost, max_cost;

	while (s->group_count[group1] + s->group_count[group2] < TOTAL) {
		/* break if the distribution is too unfair */
		if (TOTAL - s->group_count[group1] == TR_CHILD_COUNT_MIN) break;
		if (TOTAL - s->group_count[group2] == TR_CHILD_COUNT_MIN) break;

		/* find the node that requires max cost to add either group */
		max_cost = -1.0;
		for (i = 0; i < TOTAL; i++) {
			if (s->partition[i] != undecided) continue;
			if (s->nodes[i].nodeOId == UNDEF_OID) continue;

			cost = TrSelectGroup(s, &s->nodes[i].rect, &group);

			if (cost > max_cost ||
				(cost == max_cost &&
					s->group_count[group] < s->group_count[target_group])) {
				max_cost = cost;
				target_node = i;
				target_group = group;
			}
		}

		/* set target_node as group that requires lower cost */
		TrDecideGroup(s, target_node, target_group);
	}

	if (s->group_count[group1] + s->group_count[group2] < TOTAL) {
		/* categorization breaked because the distribution is too unfair;
		 * set all the rest nodes as the smaller group
		 */
		group = s->group_count[group1] >= TOTAL - TR_CHILD_COUNT_MIN ? group2
																	 : group1;
		for (i = 0; i < TOTAL; i++) {
			if (s->partition[i] == undecided) TrDecideGroup(s, i, group);
		}
	}
}

/* the overview of node split */
void TrNode_split(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	OId nOId, TrChild b, OId *nnOId) {
	UNDEF_OID_CHECK(nOId);
	NULL_PTR_CHECK(b);
	NULL_PTR_CHECK(nnOId);

	UpdateBaseObject baseObj(objectManager, strategy, nOId);
	TrNode n = baseObj.getBaseAddr<TrNode>();
	int32_t level = n->level;
	TrSplitTag s;

	/* setup two groups */
	TrSplit_initialize(&s, n, b);
	TrPickFirstSeeds(&s);

	/* distribute all nodes */
	TrCategorize(&s);

	*nnOId = TrNode_new(txn, objectManager, strategy);
	UpdateBaseObject nnBaseObj(objectManager, strategy, *nnOId);
	TrNode nn = nnBaseObj.getBaseAddr<TrNode>();
	nn->level = n->level = level;

	TrSplit_finalize(txn, objectManager, strategy, &s, nOId, *nnOId);
}
