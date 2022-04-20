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

#ifndef GIS_POINTGEOM_H_
#define GIS_POINTGEOM_H_

#include "util/container.h"

#include <sstream>

#include "expression.h"
#include "gis_geometry.h"
#include "gis_point.h"

/*!
* @brief Base class for point container classes
*
*/
class PointGeom : public Geometry {
public:
	bool isSimple() const {
		return isSimple_;
	}

	virtual GeometryType getType() const = 0;

	/*!
	 * @brief Get the in-memory size of geometry
	 *
	 * @return size
	 */
	size_t getRawSize() const {
		return sizeof(*this);
	}

	const TrRectTag &getBoundingRect() const {
		return boundingRect_;
	}

	Point &getPoint(int n) const {
		if (static_cast<size_t>(n) >= p_.size() || n < 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_GET_VALUE_FAILED,
				"Internal logic error: cannot obtain point");
		}
		return *p_[n];
	}
	const QP_XArray<Point *> &getPoints() const {
		return p_;
	}
	int numPoints() const {
		return static_cast<int>(p_.size());
	}
	bool crossGeometry() const {
		return false;
	}

	virtual PointGeom *assign(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ContainerRowWrapper *amap,
		FunctionMap *fmap, EvalMode mode = EVAL_MODE_NORMAL) {
		if (isAssigned_ || isEmpty_) {
			return static_cast<PointGeom *>(dup(txn, objectManager, strategy));
		}
		PointGeom *x = static_cast<PointGeom *>(this->dup(txn, objectManager, strategy));
		for (QP_XArray<Point *>::const_iterator it = x->p_.begin();
			 it != x->p_.end(); ++it) {
			QP_DELETE(*it);
		}

		x->p_.clear();
		for (QP_XArray<Point *>::const_iterator it = p_.begin(); it != p_.end();
			 ++it) {
			Point *p = (*it)->assign(txn, objectManager, strategy, amap, fmap, mode);
			x->p_.push_back(p);
		}
		if (!x->p_.empty() && x->isAssigned_) {
			x->calcBoundingRectAndSimplicity();
		}
		return x;
	}

	virtual void stringify(
		TransactionContext &txn, util::NormalOStringStream &os) const {
		if (isEmpty_) {
			os << "EMPTY";
		}
		else {
			for (size_t i = 0; i < p_.size(); i++) {
				p_[i]->stringify(txn, os);
				if (i < p_.size() - 1) os << ',';
			}
		}
		if (srId_ != -1) {
			os << ";" << srId_;
		}
	}

	virtual ~PointGeom() {
		for (QP_XArray<Point *>::const_iterator it = p_.begin(); it != p_.end();
			 ++it) {
			QP_DELETE(*it);
		}
	}

protected:
	explicit PointGeom(TransactionContext &txn)
		: Geometry(txn), p_(txn.getDefaultAllocator()) {
		isEmpty_ = true;
		isSimple_ = true;
		isClosed_ = false;
		p_.clear();
	}

	PointGeom(srid_t id, const QP_XArray<Point *> &parray,
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy)
		: Geometry(txn), p_(txn.getDefaultAllocator()) {
		isSimple_ = true;
		isEmpty_ = false;
		srId_ = id;
		isAssigned_ = true;
		isClosed_ = false;
		dimension_ = -1;
		for (QP_XArray<Point *>::const_iterator it = parray.begin();
			 it != parray.end(); ++it) {
			p_.push_back((*it)->dup(txn, objectManager, strategy));
			isAssigned_ &= (*it)->isAssigned();
			int d = (*it)->getDimension();
			if (dimension_ != -1 && d != dimension_) {
				GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
					"Internal logic error: "
					"Different dimensions in multipoint geometry.");
			}
			dimension_ = d;
		}
	}

	virtual size_t getSerializedSize() const {
		size_t s = sizeof(uint16_t);
		for (uint32_t i = 0; i < p_.size(); i++) {
			s += p_[i]->getSerializedSize();
		}
		return s;
	}
	void serializeObject(util::XArray<uint8_t> &out, uint64_t &offset) {
		if (isAssigned_) {
			if (dimension_ != 0) {
				uint16_t len = static_cast<uint16_t>(p_.size());
				out.push_back(
					reinterpret_cast<uint8_t *>(&len), sizeof(uint16_t));
				offset += sizeof(uint16_t);
				for (uint32_t i = 0; i < len; i++) {
					p_[i]->serializeObject(out, offset);
				}
			}
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_SERIALIZE,
				"Internal logic error: Cannot serialize geometry object");
		}
	}

	virtual void calcBoundingRectAndSimplicity() = 0;
	QP_XArray<Point *> p_;
	bool isSimple_;
	bool isClosed_;
};

#endif /* GIS_POINTGEOM_HPP */
