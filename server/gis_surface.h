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
	@brief Surface class for 3D-geometry data
*/

#ifndef GIS_SURFACE_H_
#define GIS_SURFACE_H_

#include "util/container.h"

#include "gis_geometry.h"
#include "gis_linearring.h"
#include "gis_linestring.h"
#include "gis_multipoint.h"
#include "gis_pointgeom.h"

/*!
* @brief Base class for Polygon : LinearRing container.
*/
class Surface : public Geometry {
public:
	/*!
	* @brief Get pointer to exterior border for test
	*
	* @return exterior border as LinearRing
	*/
	LinearRing *getExteriorBorder() const {
		return exteriorBorder_;
	}
	/*!
	* @brief Get pointer to internal border for test
	*
	* @param n internal border index
	* @return nth-internal border as LinearRing
	*/
	LinearRing *getInteriorBorder(int n) const {
		if (interiorBorders_ == NULL ||
			static_cast<size_t>(n) >= interiorBorders_->size()) {
			return NULL;
		}
		return (*interiorBorders_)[n];
	}

	/*
	void insertInteriorBorder(LinearRing *pRing){
	if(!isBoundingRectInclude(pRing)){
	GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_ERROR_OTHER,
	"Invalid interior border");
	}
	if(interiorBorders_ == NULL){
	interiorBorders_ = QP_NEW QP_XArray<LinearRing*>();
	}
	for(int i=0; i<interiorBorders_->size(); i++){
	if( (*interiorBorders_)[i]->isBoundingRectTouch(pRing)){
	GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_ERROR_OTHER,
	"Invalid interior border");
	}
	}
	interiorBorders_->push_back(pRing);
	}
	*/

	/*!
	* @brief Get MBB of the surface
	*
	* @return MBB rect
	*/
	const TrRectTag &getBoundingRect() const {
		if (exteriorBorder_ != NULL) {
			return exteriorBorder_->getBoundingRect();
		}
		else {
			return boundingRect_;
		}
	}

protected:
	/*!
	* @brief Constructor for empty object
	*/
	explicit Surface(TransactionContext &txn) : Geometry(txn) {
		isEmpty_ = true;
		isSimple_ = true;
		isClosed_ = false;
		exteriorBorder_ = NULL;
		interiorBorders_ = NULL;
		boundingRect_.xmin = boundingRect_.ymin = boundingRect_.zmin =
			boundingRect_.xmax = boundingRect_.ymax = boundingRect_.zmax =
				std::numeric_limits<double>::quiet_NaN();
	}

	/*!
	* @brief Constructor
	*
	* @param id SRID
	* @param exteriorBorder exterior border LinearRing to duplicate
	* @param interiorBorders interior border vector of LinearRing to duplicate
	* @param txn The transaction context
	* @param txn Object manager
	*/
	Surface(srid_t id, LinearRing *exteriorBorder,
		QP_XArray<LinearRing *> *interiorBorders, TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy)
		: Geometry(txn) {
		setBorders(txn, objectManager, strategy, exteriorBorder, interiorBorders);
		srId_ = id;
		isSimple_ = true;
		isClosed_ = false;
		boundingRect_.xmin = boundingRect_.ymin = boundingRect_.zmin =
			boundingRect_.xmax = boundingRect_.ymax = boundingRect_.zmax =
				std::numeric_limits<double>::quiet_NaN();
	}

	/*!
	* @brief Set new border
	*
	* @param txn The transaction context
	* @param txn Object manager
	* @param exteriorBorder exterior border LinearRing to duplicate
	* @param interiorBorders interior border vector of LinearRing to duplicate
	*/
	void setBorders(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		LinearRing *exteriorBorder, QP_XArray<LinearRing *> *interiorBorders) {
		if (exteriorBorder != NULL) {
			exteriorBorder_ = exteriorBorder->dup(txn, objectManager, strategy);
		}
		else {
			exteriorBorder_ = NULL;
		}
		if (interiorBorders != NULL) {
			interiorBorders_ =
				QP_NEW QP_XArray<LinearRing *>(txn.getDefaultAllocator());
			QP_XArray<LinearRing *>::const_iterator it =
				interiorBorders->begin();
			while (it != interiorBorders->end()) {
				interiorBorders_->push_back((*it)->dup(txn, objectManager, strategy));
				++it;
			}
		}
		else {
			interiorBorders_ = NULL;
		}
		if (exteriorBorder) {
			dimension_ = exteriorBorder->dimension_;
		}
		else {
			dimension_ = 0;
		}
	}

	/*!
	* @brief Destructor: Not needed if stack allocator is used.
	*/
	virtual ~Surface() {
		QP_SAFE_DELETE(exteriorBorder_);
		if (interiorBorders_ != NULL) {
			while (!interiorBorders_->empty()) {
				QP_SAFE_DELETE(interiorBorders_->back());
				interiorBorders_->pop_back();
			}
			QP_SAFE_DELETE(interiorBorders_);
		}
	}

	virtual size_t getSerializedSize() const {
		size_t s = sizeof(uint16_t);
		s += exteriorBorder_->getSerializedSize();
		if (interiorBorders_) {
			for (uint16_t i = 0; i < interiorBorders_->size(); i++) {
				s += (*interiorBorders_)[i]->getSerializedSize();
			}
		}
		return s;
	}

	void serializeObject(util::XArray<uint8_t> &out, uint64_t &offset) {
		if (dimension_ != 0) {
			uint16_t nInterior;
			if (interiorBorders_ == NULL) {
				nInterior = 0;
			}
			else {
				nInterior = static_cast<uint16_t>(interiorBorders_->size());
			}
			out.push_back(
				reinterpret_cast<uint8_t *>(&nInterior), sizeof(uint16_t));
			offset += sizeof(uint16_t);
			exteriorBorder_->serializeObject(out, offset);
			if (interiorBorders_) {
				for (uint16_t i = 0; i < nInterior; i++) {
					(*interiorBorders_)[i]->serializeObject(out, offset);
				}
			}
		}
	}

	LinearRing *exteriorBorder_;
	QP_XArray<LinearRing *> *interiorBorders_;
	bool isSimple_;
	bool isClosed_;
	TrRectTag boundingRect_;
};

#endif /* GIS_SURFACE_H_*/
