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
	@brief Polygon-collection-based class for 3D-geometry data
*/

#ifndef GIS_MULTIPOLYGON_H_
#define GIS_MULTIPOLYGON_H_

#include <sstream>

#include "util/container.h"

#include "expression.h"
#include "gis_geometry.h"
#include "gis_point.h"
#include "gis_pointgeom.h"
#include "gis_polygon.h"
#include "transaction_context.h"

/*!
 * @brief Class for point list
 *
 */
class MultiPolygon : public Geometry {
	friend class PolyhedralSurface;

public:
	explicit MultiPolygon(TransactionContext &txn)
		: Geometry(txn), polygonArray_(txn.getDefaultAllocator()) {
		isSimple_ = true;
		srId_ = -1;
		isAssigned_ = true;
		isEmpty_ = true;
		isClosed_ = false;
	}

	~MultiPolygon() {}

	MultiPolygon(srid_t id, const QP_XArray<Polygon *> &parray,
		TransactionContext &txn, ObjectManager &objectManager)
		: Geometry(txn), polygonArray_(txn.getDefaultAllocator()) {
		isSimple_ = true;
		isEmpty_ = false;
		srId_ = id;
		isAssigned_ = true;
		dimension_ = -1;
		for (QP_XArray<Polygon *>::const_iterator it = parray.begin();
			 it != parray.end(); ++it) {
			polygonArray_.push_back((*it)->dup(txn, objectManager));
			isAssigned_ &= (*it)->isAssigned();
			int d = (*it)->getDimension();
			if (dimension_ != -1 && d != dimension_) {
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: "
					"Different dimensions in multipoint geometry.");
			}
			dimension_ = d;
		}
		if (!polygonArray_.empty() && isAssigned_) {
			calcBoundingRectAndSimplicity();
		}
		isClosed_ = false;  
	}
	virtual GeometryType getType() const {
		return MULTIPOLYGON;
	}
	/*!
	 * @brief Get the in-memory size of geometry
	 *
	 * @return type
	 */
	size_t getRawSize() const {
		return sizeof(*this);
	}

	MultiPolygon *dup(
		TransactionContext &txn, ObjectManager &objectManager, srid_t id) {
		if (isEmpty()) {
			return QP_NEW MultiPolygon(txn);
		}
		else {
			MultiPolygon *mp =
				QP_NEW MultiPolygon(id, polygonArray_, txn, objectManager);
			mp->isAssigned_ = isAssigned_;
			mp->isEmpty_ = isEmpty_;
			return mp;
		}
	}
	MultiPolygon *dup(TransactionContext &txn, ObjectManager &objectManager) {
		return dup(txn, objectManager, srId_);
	}

	virtual const char *getString(TransactionContext &txn) const {
		util::NormalOStringStream os;
		os << "MULTIPOLYGON(";
		stringify(txn, os);
		os << ')';
		std::string str = os.str();
		const size_t len = str.size();
		char *ret = static_cast<char *>(QP_ALLOCATOR.allocate(len + 1));
		memcpy(ret, str.c_str(), len);
		ret[len] = '\0';
		return ret;
	}

	virtual bool operator==(MultiPolygon &) const {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
			"Internal logic error: not implemented");
	}
	virtual bool operator==(Geometry &g) const {
		GeometryType t = g.getType();
		if (t != MULTIPOLYGON && !(t == POLYGON && polygonArray_.size() == 1)) {
			return false;
		}
		return *this == dynamic_cast<MultiPolygon &>(g);
	}

	bool isSimple() const {
		return isSimple_;
	}

	virtual MultiPolygon *assign(TransactionContext &txn,
		ObjectManager &objectManager, ContainerRowWrapper *amap,
		FunctionMap *fmap, EvalMode mode = EVAL_MODE_NORMAL) {
		if (isAssigned_ || isEmpty_) {
			return dup(txn, objectManager);
		}

		QP_XArray<Polygon *> newPolygonArray(txn.getDefaultAllocator());
		if (!polygonArray_.empty()) {
			for (size_t k = 0; k < polygonArray_.size(); k++) {
				newPolygonArray.push_back(
					static_cast<Polygon *>(polygonArray_[k]->assign(
						txn, objectManager, amap, fmap, mode)));
			}
		}

		MultiPolygon *p =
			QP_NEW MultiPolygon(srId_, newPolygonArray, txn, objectManager);

		while (!newPolygonArray.empty()) {
			QP_DELETE(newPolygonArray.back());
			newPolygonArray.pop_back();
		}

		return p;
	}

	/*!
	* @brief Get MBB of the surface
	*
	* @return MBB rect
	*/
	const TrRectTag &getBoundingRect() const {
		return this->boundingRect_;
	}

	virtual void stringify(
		TransactionContext &txn, util::NormalOStringStream &os) const {
		if (isEmpty_) {
			os << "EMPTY";
		}
		else {
			for (size_t i = 0; i < polygonArray_.size(); i++) {
				os << '(';
				polygonArray_[i]->stringify(txn, os);
				os << ')';
				if (i < polygonArray_.size() - 1) os << ',';
			}
		}
		if (srId_ != -1) {
			os << ";" << srId_;
		}
	}

protected:
	void calcBoundingRectAndSimplicity() {
		if (polygonArray_.empty()) {
			boundingRect_.xmin = boundingRect_.ymin = boundingRect_.zmin =
				boundingRect_.xmax = boundingRect_.ymax = boundingRect_.zmax =
					std::numeric_limits<double>::quiet_NaN();
			return;
		}
		size_t i;
		boundingRect_ = polygonArray_[0]->getBoundingRect();
		for (i = 1; i < polygonArray_.size(); i++) {
			TrRectTag tmpRect = polygonArray_[i]->getBoundingRect();
			boundingRect_.xmin = std::min(boundingRect_.xmin, tmpRect.xmin);
			boundingRect_.ymin = std::min(boundingRect_.ymin, tmpRect.ymin);
			boundingRect_.zmin = std::min(boundingRect_.zmin, tmpRect.zmin);
			boundingRect_.xmax = std::max(boundingRect_.xmax, tmpRect.xmax);
			boundingRect_.ymax = std::max(boundingRect_.ymax, tmpRect.ymax);
			boundingRect_.zmax = std::max(boundingRect_.zmax, tmpRect.zmax);
		}
		isSimple_ = true;  
	}

	virtual size_t getSerializedSize() const {
		size_t s = sizeof(uint16_t);
		for (uint16_t i = 0; i < polygonArray_.size(); i++) {
			s += polygonArray_[i]->getSerializedSize();
		}
		return s;
	}

	void serializeObject(util::XArray<uint8_t> &out, uint64_t &offset) {
		uint16_t nPolygon = static_cast<uint16_t>(polygonArray_.size());
		out.push_back(reinterpret_cast<uint8_t *>(&nPolygon), sizeof(uint16_t));
		offset += sizeof(uint16_t);
		for (uint16_t i = 0; i < nPolygon; i++) {
			polygonArray_[i]->serializeObject(out, offset);
		}
	}

	static MultiPolygon *deserializeObject(TransactionContext &txn, uint8_t *in,
		uint64_t &offset, int srId, int dimension) {
		MultiPolygon *p = QP_NEW MultiPolygon(txn);
		uint16_t nPolygon = *reinterpret_cast<uint16_t *>(in + offset);
		offset += sizeof(uint16_t);
		p->dimension_ = dimension;
		for (uint16_t i = 0; i < nPolygon; i++) {
			p->polygonArray_.push_back(
				Polygon::deserializeObject(txn, in, offset, srId, dimension));
		}
		p->calcBoundingRectAndSimplicity();
		p->isEmpty_ = (dimension == 0) || (nPolygon == 0);
		return p;
	}

	QP_XArray<Polygon *> polygonArray_;
	bool isSimple_;
	bool isClosed_;
};

#endif /* GIS_MULTIPOLYGON_H_*/
