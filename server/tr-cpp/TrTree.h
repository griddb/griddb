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

#ifndef TR_H
#define TR_H

#include "util/type.h"

#include "TrPv3.h"

typedef struct _TrIndexTag *TrIndex;
typedef struct _TrVectorTag { double x, y, z; } TrVectorTag, *TrVector;
typedef struct _TrRectTag {
	double xmin, ymin, zmin;
	double xmax, ymax, zmax;
} TrRectTag, *TrRect;

#include "object_manager.h"
#include "transaction_context.h"

typedef int32_t (*TrCheckCallback)(TransactionContext &txn, TrRect, void *);
typedef int32_t (*TrHitCallback)(TransactionContext &txn, TrRect, OId, void *);
typedef int32_t (*TrDataCmpCallback)(
	TransactionContext &txn, TrRect, OId, void *);

extern OId TrIndex_new(TransactionContext &txn, ObjectManager &objectManager);
extern void TrIndex_destroy(
	TransactionContext &txn, ObjectManager &objectManager, OId idxOId, uint64_t &removeNum);
extern int32_t TrIndex_size(
	TransactionContext &txn, ObjectManager &objectManager, OId idxOId);
extern int32_t TrIndex_insert(TransactionContext &txn,
	ObjectManager &objectManager, OId idxOId, TrRect r, OId dataOId);
extern int32_t TrIndex_delete(TransactionContext &txn,
	ObjectManager &objectManager, OId idxOId, TrRect r, void *data);
extern int32_t TrIndex_delete_cmp(TransactionContext &txn,
	ObjectManager &objectManager, OId idxOId, TrRect r, TrDataCmpCallback dccb,
	void *dccbarg);

extern void TrIndex_search(TransactionContext &txn,
	ObjectManager &objectManager, OId idxOId, TrCheckCallback ccb, void *ccbarg,
	TrHitCallback hcb, void *hcbarg);

extern void TrIndex_all(TransactionContext &txn, ObjectManager &objectManager,
	OId idxOId, TrHitCallback cb, void *cbarg);
extern void TrIndex_search_rect(TransactionContext &txn,
	ObjectManager &objectManager, OId idxOId, TrRect r, TrHitCallback cb,
	void *cbarg);
extern void TrIndex_search_quad(TransactionContext &txn,
	ObjectManager &objectManager, OId idxOId, TrPv3Key *qkey, TrHitCallback cb,
	void *cbarg);

extern void TrIndex_dump(
	TransactionContext &txn, ObjectManager &objectManager, OId idxOId);
#endif
