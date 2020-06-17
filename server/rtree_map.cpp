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
	@brief Rtree map implementation
*/

#include "rtree_map.h"
#include "util/type.h"
#include "util/container.h"
#include "TrTree.h"
#include "collection.h"
#include "data_store.h"
#include "data_type.h"
#include "gis_geometry.h"
#include "gs_error.h"
#include "qp_def.h"
#include "time_series.h"
#include "value.h"
#include <iostream>

/*!
 * @brief Initialize already memcpy-ed region
 *
 * @param txn TransactionContext
 * @param columnType COLUMN_TYPE_GEOMETRY
 * @param columnId ColumnId
 * @param metaAllocateStrategy AllocateStrategy for RtreeMapImage
 * @param isUnique specifies whether the key is unique
 *
 * @return 0 if succeeded.
 * @note returns void?
 */
int32_t RtreeMap::initialize(TransactionContext &txn, ColumnType columnType,
	ColumnId columnId, const AllocateStrategy &metaAllocateStrategy,
	bool isUnique) {
	if (columnType != COLUMN_TYPE_GEOMETRY) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_CANNOT_MAKE_INDEX,
			"Cannot use RtreeMap with non-geometry column");
	}
	rtreeMapImage_ = BaseObject::allocate<RtreeMapImage>(sizeof(RtreeMapImage),
		metaAllocateStrategy, getBaseOId(), OBJECT_TYPE_RTREE_MAP);

	rtreeMapImage_->columnId_ = columnId;
	rtreeMapImage_->isUnique_ = isUnique;
	rtreeMapImage_->size_ = 0;
	rtreeMapImage_->offset_ = 0;

	rtreeMapImage_->oId_ = TrIndex_new(txn, *getObjectManager());

	return 0;
}

/*!
 * @brief Nothing to do
 *
 * @param txn Transaction Context
 *
 * @return void
 */
bool RtreeMap::finalize(TransactionContext &txn) {
	setDirty();

	uint64_t removeNum = NUM_PER_EXEC;
	if (rtreeMapImage_->oId_ != UNDEF_OID) {
		TrIndex_destroy(txn, *getObjectManager(), rtreeMapImage_->oId_, removeNum);
		if (removeNum > 0) {
			rtreeMapImage_->oId_ = UNDEF_OID;
			rtreeMapImage_->size_ = 0;
		}
	}
	if (removeNum > 0) {
		BaseObject::finalize();
	}
	return removeNum > 0;
}

/*!
 * @brief Returns size
 *
 * @return size
 */
uint64_t RtreeMap::size() {
	return rtreeMapImage_->size_;
}

/*!
 * @brief Get geometry object from OId
 *
 * @param txn Transaction Context
 * @param constKey Search key
 *
 * @return Found geometry
 */
Geometry *RtreeMap::getGeometry(TransactionContext &txn, const void *constKey) {
	if (constKey == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_BAD_ADDRESS,
			"RtreeMap cannot obtain correct geometry object");
	}

	void *key = const_cast<void *>(constKey);
	BinaryObject tmpVariant(reinterpret_cast<uint8_t *>(key));
	return Geometry::deserialize(txn, tmpVariant.data(), tmpVariant.size());
}

/*!
 * @brief Insert an oId into the index.
 *
 * @param txn Transaction Context
 * @param key Search key
 * @param oId an Object-ID of a row image
 *
 * @return -1 if failed, 0 otherwise
 */
int32_t RtreeMap::insert(TransactionContext &txn, const void *key, OId oId) {
	setDirty();

	TrRectTag r;
	r = getGeometry(txn, key)->getBoundingRect();
	TrIndex_insert(txn, *getObjectManager(), rtreeMapImage_->oId_, &r, oId);
	++rtreeMapImage_
		  ->size_;  

	return 0;
}

/*!
 * @brief Removes an oId from the index
 *
 * @param txn Transaction Context
 * @param key Search key
 * @param oId an Object-ID of a row image to delete
 *
 * @return -1 if failed, 0 otherwise
 */
int32_t RtreeMap::remove(TransactionContext &txn, const void *key, OId oId) {
	setDirty();

	TrRectTag r;
	r = getGeometry(txn, key)->getBoundingRect();
	if (TrIndex_delete_cmp(txn, *getObjectManager(), rtreeMapImage_->oId_, &r,
			oIdCmpCallback, &oId)) {
		--rtreeMapImage_->size_;  
	}

	return 0;
}

/*!
 * @brief Changes an oId of the specified node in the index.
 *
 * @param txn Transaction Context
 * @param key Search key
 * @param oId old row-image
 * @param newOId new row-image
 *
 * @return -1 if failed.
 */
int32_t RtreeMap::update(
	TransactionContext &txn, const void *key, OId oId, OId newOId) {
	setDirty();

	TrRectTag r;
	r = getGeometry(txn, key)->getBoundingRect();
	TrIndex_delete_cmp(txn, *getObjectManager(), rtreeMapImage_->oId_, &r,
		oIdCmpCallback, &oId);
	TrIndex_insert(txn, *getObjectManager(), rtreeMapImage_->oId_, &r, newOId);

	return 0;
}

/*!
 * @brief Retrieves all data in the rtree index.
 *
 * @param txn Transaction Context
 * @param limit limit number
 * @param idList answer vector
 *
 * @return void
 */
int32_t RtreeMap::getAll(
	TransactionContext &txn, ResultSize limit, util::XArray<OId> &idList) {

	HitIntersectCallbackArg arg;
	arg.limit = limit;
	arg.oidList = &idList;
	arg.size = 0;

	TrIndex_all(
		txn, *getObjectManager(), rtreeMapImage_->oId_, hitAllCallback, &arg);

	return 0;
}

/*!
 * @brief Searches an oId, according to the key
 *
 * @param txn Transaction Context
 * @param key Non-empty geometry which has its bounding box
 * @param oId answer
 *
 * @return void
 */
int32_t RtreeMap::search(
	TransactionContext &txn, const void *key, OId &oId) {

	const Geometry *g = reinterpret_cast<const Geometry *>(key);
	HitIntersectCallbackArg arg;
	TrRectTag r = g->getBoundingRect();
	arg.rect = &r;
	arg.limit = 1;
	arg.size = UINT64_MAX;  
	arg.oidList = NULL;
	arg.oneOId = UNDEF_OID;
	TrIndex_search(txn, *getObjectManager(), rtreeMapImage_->oId_,
		checkCallback, &r, hitIntersectCallback, &arg);
	oId = arg.oneOId;
	return 0;
}

/*!
 * @brief Search the index according to the condition
 *
 * @param txn Transaction Context
 * @param sc search condition
 * @param oidList answer vector
 */
void RtreeMap::search(TransactionContext &txn, RtreeMap::SearchContext &sc,
	util::XArray<OId> &oidList, OutputOrder outputOrder) {
	UNUSED_VARIABLE(outputOrder);

	TermCondition *cond = sc.getKeyCondition();
	SearchContext::GeomeryCondition *geomCond = 
		static_cast<SearchContext::GeomeryCondition *>(
			const_cast<void *>(cond->value_));
	if (geomCond->valid_) {
		getAll(txn, sc.getLimit(), oidList);
	}
	else {
		switch (geomCond->relation_) {
		case GEOMETRY_INTERSECT: {
			HitIntersectCallbackArg arg;
			arg.rect = &(geomCond->rect_[0]);
			arg.limit = static_cast<ResultSize>(sc.getLimit());
			arg.oidList = &oidList;
			arg.size = 0;
			TrIndex_search(txn, *getObjectManager(), rtreeMapImage_->oId_,
				checkCallback, arg.rect, hitIntersectCallback, &arg);
			break;
		}
		case GEOMETRY_INCLUDE: {
			HitIncludeCallbackArg arg;
			arg.rect = &(geomCond->rect_[0]);
			arg.limit = static_cast<ResultSize>(sc.getLimit());
			arg.oidList = &oidList;
			arg.size = 0;
			TrIndex_search(txn, *getObjectManager(), rtreeMapImage_->oId_,
				checkCallback, arg.rect, hitIncludeCallback, &arg);
			break;
		}
		case GEOMETRY_DIFFERENTIAL: {
			HitDifferentialCallbackArg arg;
			arg.rect1 = &(geomCond->rect_[0]);
			arg.rect2 = &(geomCond->rect_[1]);
			arg.limit = static_cast<ResultSize>(sc.getLimit());
			arg.oidList = &oidList;
			arg.size = 0;
			TrIndex_search(txn, *getObjectManager(), rtreeMapImage_->oId_,
				checkCallback, arg.rect1, hitDifferentialCallback, &arg);
			break;
		}
		case GEOMETRY_QSF_INTERSECT: {
			HitQsfIntersectCallbackArg arg;
			arg.pkey = &(geomCond->pkey_);
			arg.limit = static_cast<ResultSize>(sc.getLimit());
			arg.oidList = &oidList;
			arg.size = 0;
			TrIndex_search_quad(txn, *getObjectManager(), rtreeMapImage_->oId_,
				arg.pkey, hitQsfIntersectCallback, &arg);
			break;
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_UNKNOWN_RELATIONSHIP,
				"Unknown relationship of RtreeMap.");
		}
	}
}

/*!
 * @brief TrIndex check callback for intersection.
 *
 * @param txn Transaction Context
 * @param r Rect to check
 * @param arg argument pointer (TrRect)
 *
 * @return 1 if search continues to the below (leaf), 0 if search returns to the
 * above
 */
int32_t RtreeMap::checkCallback(TransactionContext &, TrRect r, void *arg) {
	TrRect argRect = reinterpret_cast<TrRect>(arg);
	return (rectCheckIntersect(r, argRect)) ? 1 : 0;
}

/*!
 * @brief Hit callback to scan full data
 *
 * @param hitOId Data on leaf
 * @param arg_ pointer to HitIntersectCallbackArg
 *
 * @return 1 if search continues, 0 if search finishes
 */
int32_t RtreeMap::hitAllCallback(
	TransactionContext &, TrRect, OId hitOId, void *arg_) {
	HitIntersectCallbackArg *arg =
		reinterpret_cast<HitIntersectCallbackArg *>(arg_);
	if (arg->limit <= arg->size && arg->size != UINT64_MAX) {
		return 0;
	}

	if (arg->size == UINT64_MAX) {
		arg->oneOId = hitOId;
		arg->size = 1;
		return 0;
	}
	else {
		arg->size++;
		arg->oidList->push_back(hitOId);
		return 1;
	}
}

/*!
 * @brief Hit callback to check intersection
 *
 * @param r Key-rect on leaf
 * @param hitOId Data on leaf
 * @param arg_ pointer to HitIntersectCallbackArg
 *
 * @return 1 if search continues, 0 if search finishes
 */
int32_t RtreeMap::hitIntersectCallback(
	TransactionContext &, TrRect r, OId hitOId, void *arg_) {
	HitIntersectCallbackArg *arg =
		reinterpret_cast<HitIntersectCallbackArg *>(arg_);
	if (arg->limit <= arg->size && arg->size != UINT64_MAX) {
		return 0;
	}

	if (rectCheckIntersect(arg->rect, r)) {
		if (arg->size == UINT64_MAX) {
			arg->oneOId = hitOId;
			arg->size = 1;
			return 0;
		}
		else {
			arg->size++;
			arg->oidList->push_back(hitOId);
		}
	}
	return 1;
}

/*!
 * @brief Hit callback to check inclusion
 *
 * @param r Key-rect on leaf
 * @param hitOId Data on leaf
 * @param arg_ Pointer to HitIncludeCallbackArg
 *
 * @return 1 if search continues, 0 if search finishes
 */
int32_t RtreeMap::hitIncludeCallback(
	TransactionContext &, TrRect r, OId hitOId, void *arg_) {
	HitIncludeCallbackArg *arg =
		reinterpret_cast<HitIncludeCallbackArg *>(arg_);
	if (arg->limit <= arg->size && arg->size != UINT64_MAX) {
		return 0;
	}

	if (rectCheckInclude(arg->rect, r)) {
		if (arg->size == UINT64_MAX) {
			arg->oneOId = hitOId;
			arg->size = 1;
			return 0;
		}
		else {
			arg->size++;
			arg->oidList->push_back(hitOId);
		}
	}

	return 1;
}

/*!
 * @brief Hit callback for differentiality
 *
 * @param r Key-rect on leaf
 * @param hitOId Data on leaf
 * @param arg_ Pointer to HitDifferntialCallbackArg
 *
 * @return 1 if search continues, 0 if search finishes
 */
int32_t RtreeMap::hitDifferentialCallback(
	TransactionContext &, TrRect r, OId hitOId, void *arg_) {
	HitDifferentialCallbackArg *arg =
		reinterpret_cast<HitDifferentialCallbackArg *>(arg_);
	if (arg->limit <= arg->size && arg->size != UINT64_MAX) {
		return 0;
	}

	if (rectCheckIntersect(arg->rect1, r) &&
		!rectCheckIntersect(arg->rect2, r)) {
		arg->size++;
		arg->oidList->push_back(hitOId);
	}

	return 1;
}

/*!
 * @brief Hit callback to check qsf intersection
 *
 * @param r Key-rect on leaf
 * @param hitOId Data on leaf
 * @param arg_ pointer to HitQsfIntersectCallbackArg
 *
 * @return 1 if search continues, 0 if search finishes
 */
int32_t RtreeMap::hitQsfIntersectCallback(
	TransactionContext &, TrRect r, OId hitOId, void *arg_) {
	HitQsfIntersectCallbackArg *arg =
		reinterpret_cast<HitQsfIntersectCallbackArg *>(arg_);
	if (arg->limit <= arg->size && arg->size != UINT64_MAX) {
		return 0;
	}

	TrPv3Key *qkey = arg->pkey;
	TrPv3Box box;

	box.p0[0] = r->xmin;
	box.p0[1] = r->ymin;
	box.p0[2] = r->zmin;
	box.p1[0] = r->xmax - r->xmin;
	box.p1[1] = r->ymax - r->ymin;
	box.p1[2] = r->zmax - r->zmin;


	if (TrPv3Test2(&box, qkey)) {
		arg->size++;
		arg->oidList->push_back(hitOId);
	}

	return 1;
}

/*!
 * @brief Callback to compare OId
 *
 * @param r not used
 * @param oId op1 to compare
 * @param arg pointer to op2
 */
int32_t RtreeMap::oIdCmpCallback(
	TransactionContext &, TrRect, OId oId, void *arg) {
	OId *p = reinterpret_cast<OId *>(arg);
	return static_cast<int32_t>(*p == oId);
}

bool geomOperation(TransactionContext& txn, uint8_t const* p,
				   uint32_t size1, uint8_t const* q, uint32_t size2) {
	UNUSED_VARIABLE(size2);
	Geometry *geom = Geometry::deserialize(txn, p, size1);
	const RtreeMap::SearchContext::GeomeryCondition *geomCond = 
		reinterpret_cast<const RtreeMap::SearchContext::GeomeryCondition *>(q);
	assert(geom != NULL);

	bool isMatch = true;
	switch (geomCond->relation_) {
	case GEOMETRY_INTERSECT:
		if (geom->isEmpty()) {
			isMatch = false;
		}
		else {
			const TrRectTag &a = geomCond->rect_[0];
			const TrRectTag &b = geom->getBoundingRect();
			isMatch = ((a.xmin <= b.xmax) && (b.xmin <= a.xmax) &&
					   ((a.ymin <= b.ymax) && (b.ymin <= a.ymax)) &&
					   (geom->getDimension() <= 2 ||
						   ((a.zmin <= b.zmax) && (b.zmin <= a.zmax))));
		}
		break;
	case GEOMETRY_INCLUDE:
		if (geom->isEmpty()) {
			isMatch = false;
		}
		else {
			const TrRectTag &out = geomCond->rect_[0];
			const TrRectTag &in = geom->getBoundingRect();
			isMatch = (out.xmin <= in.xmin && out.xmax >= in.xmax &&
					   out.ymin <= in.ymin && out.ymax >= in.ymax &&
					   (geom->getDimension() <= 2 ||
						   (out.zmin <= in.zmin && out.zmax >= in.zmax)));
		}
		break;
	case GEOMETRY_DIFFERENTIAL:
		if (geom->isEmpty()) {
			isMatch = false;
		}
		else {
			const TrRectTag &a = geomCond->rect_[0];
			const TrRectTag &a2 = geomCond->rect_[1];
			const TrRectTag &b = geom->getBoundingRect();
			bool conditionFlag1 =
				((a.xmin <= b.xmax) && (b.xmin <= a.xmax) &&
					((a.ymin <= b.ymax) && (b.ymin <= a.ymax)) &&
					(geom->getDimension() <= 2 ||
						((a.zmin <= b.zmax) && (b.zmin <= a.zmax))));
			bool conditionFlag2 =
				((a2.xmin <= b.xmax) && (b.xmin <= a2.xmax) &&
					((a2.ymin <= b.ymax) && (b.ymin <= a2.ymax)) &&
					(geom->getDimension() <= 2 ||
						((a2.zmin <= b.zmax) && (b.zmin <= a2.zmax))));
			isMatch = conditionFlag1 && !conditionFlag2;
		}
		break;
	case GEOMETRY_QSF_INTERSECT:

		if (geom->isEmpty()) {
			isMatch = false;
		}
		else {
			const TrRectTag r = geom->getBoundingRect();
			TrPv3Box box;
			box.p0[0] = r.xmin;
			box.p0[1] = r.ymin;
			box.p0[2] = r.zmin;
			box.p1[0] = r.xmax - r.xmin;
			box.p1[1] = r.ymax - r.ymin;
			box.p1[2] = r.zmax - r.zmin;
			isMatch =
				(TrPv3Test2(&box, const_cast<TrPv3Key *>(&(geomCond->pkey_))) != 0);
		}
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_UNKNOWN_RELATIONSHIP,
			"Invalid relationship of Geometry is specified.");
	}
	return isMatch;
}
