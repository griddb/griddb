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
	@brief Point-collection-based class for 3D-geometry data
*/

#ifndef GIS_MULTIPOINT_HPP
#define GIS_MULTIPOINT_HPP

#include <sstream>

#include "util/container.h"

#include "expression.h"
#include "gis_geometry.h"
#include "gis_point.h"
#include "gis_pointgeom.h"

/*!
 * @brief Class for point list
 *
 */
class MultiPoint : public PointGeom {
public:
	explicit MultiPoint(TransactionContext &txn) : PointGeom(txn) {
		isSimple_ = true;
		srId_ = -1;
		isAssigned_ = true;
		isEmpty_ = true;
	}

	~MultiPoint() {}

	MultiPoint(srid_t id, const QP_XArray<Point *> &parray,
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy)
		: PointGeom(id, parray, txn, objectManager, strategy) {
		if (!parray.empty() && isAssigned_) {
			calcBoundingRectAndSimplicity();
		}
	}
	virtual GeometryType getType() const {
		return MULTIPOINT;
	}
	/*!
	 * @brief Get the in-memory size of geometry
	 *
	 * @return type
	 */
	size_t getRawSize() const {
		return sizeof(*this);
	}

	MultiPoint *dup(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, srid_t id) {
		if (isEmpty()) {
			return QP_NEW MultiPoint(txn);
		}
		else {
			MultiPoint *mp = QP_NEW MultiPoint(id, p_, txn, objectManager, strategy);
			mp->isAssigned_ = isAssigned_;
			mp->isEmpty_ = isEmpty_;
			return mp;
		}
	}
	MultiPoint *dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		return dup(txn, objectManager, strategy, srId_);
	}

	const char *getString(TransactionContext &txn) const {
		util::NormalOStringStream os;
		os << "MULTIPOINT(";
		stringify(txn, os);
		os << ')';
		std::string str = os.str();
		const size_t len = str.size();
		char *ret = static_cast<char *>(QP_ALLOCATOR.allocate(len + 1));
		memcpy(ret, str.c_str(), len);
		ret[len] = '\0';
		return ret;
	}

	virtual bool operator==(MultiPoint &) const {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
			"Internal logic error: not implemented");
	}
	virtual bool operator==(Geometry &g) const {
		GeometryType t = g.getType();
		if (t != MULTIPOINT && !(t == POINT && this->p_.size() == 1)) {
			return false;
		}
		return *this == dynamic_cast<MultiPoint &>(g);
	}

protected:
	void calcBoundingRectAndSimplicity() {
		size_t i;
		boundingRect_.xmin = p_[0]->x();
		boundingRect_.ymin = p_[0]->y();
		boundingRect_.xmax = p_[0]->x();
		boundingRect_.ymax = p_[0]->y();
		if (dimension_ >= 3) {
			boundingRect_.zmin = p_[0]->z();
			boundingRect_.zmax = p_[0]->z();
		}
		else {
			boundingRect_.zmin = -std::numeric_limits<double>::infinity();
			boundingRect_.zmax = std::numeric_limits<double>::infinity();
		}
		for (i = 1; i < p_.size(); i++) {
			boundingRect_.xmin = std::min(boundingRect_.xmin, p_[i]->x());
			boundingRect_.ymin = std::min(boundingRect_.ymin, p_[i]->y());
			boundingRect_.xmax = std::max(boundingRect_.xmax, p_[i]->x());
			boundingRect_.ymax = std::max(boundingRect_.ymax, p_[i]->y());
			if (dimension_ >= 3) {
				boundingRect_.zmin = std::min(boundingRect_.zmin, p_[i]->z());
				boundingRect_.zmax = std::max(boundingRect_.zmax, p_[i]->z());
			}
			for (size_t k = 0; isSimple_ && k < i; k++) {
				isSimple_ = (*p_[k] != *p_[i]);
			}
		}
	}
};

#endif /* GIS_MULTIPOINT_HPP */
