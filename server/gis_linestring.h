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
	@brief LineString class for 3D-geometry data
*/

#ifndef GIS_LINESTRING_H_
#define GIS_LINESTRING_H_

#include "util/container.h"

#include "gis_pointgeom.h"

class Polygon;
class Surface;

/*!
 * @brief Linestring class definition
 *
 */
class LineString : public PointGeom {
	friend class Polygon;
	friend class Surface;
	friend class Geometry;

public:
	/*!
	 * @brief Constructor for empty object
	 * @param txn The transaction context
	 *
	 */
	LineString(TransactionContext &txn) : PointGeom(txn) {
		isEmpty_ = true;
		isSimple_ = true;
		isClosed_ = false;
		isAssigned_ = true;
		srId_ = -1;
	}

	/*!
	 * @brief Constructor
	 *
	 * @param id SRID
	 * @param parray a list of point objects
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 */
	LineString(srid_t id, const QP_XArray<Point *> &parray,
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy)
		: PointGeom(id, parray, txn, objectManager, strategy) {
		if (parray.size() < 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_MAKE_OBJECT,
				"Cannot create linestring from one point.");
		}
		isClosed_ = (*parray[parray.size() - 1] == *parray[0]);
		isEmpty_ = false;
		if (isAssigned_) {
			calcBoundingRectAndSimplicity();
		}
	}

	/*!
	 * @brief Constructor for two points (syntax suger)
	 *
	 * @param id SRID
	 * @param p1 Start point
	 * @param p2 End point
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 */
	LineString(srid_t id, Point &p1, Point &p2, TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy)
		: PointGeom(txn) {
		srId_ = id;
		p_.push_back(p1.dup(txn, objectManager, strategy));
		p_.push_back(p2.dup(txn, objectManager, strategy));
		isClosed_ = false;
		isEmpty_ = false;
		isAssigned_ = (p1.isAssigned_ && p2.isAssigned_);
		if (p1.dimension_ != p2.dimension_) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_LOGIC_ERROR,
				"Internal logic error: "
				"Different dimensions in multipoint geometry.");
		}
		dimension_ = p1.dimension_;

		if (isAssigned_) {
			calcBoundingRectAndSimplicity();
		}
	}

	/*!
	 * @brief Check for the linestring is closed
	 *
	 * @return result
	 */
	bool isClosed() const {
		return isClosed_;
	}

	/*!
	 * @brief Check for the linestring is a single line
	 *
	 * @return result
	 */
	bool isLine() const {
		return p_.size() == 2;
	}

	/*!
	 * @brief Check for the linestring is a linear line.
	 *
	 * @return result
	 */
	bool isLinearRing() const {
		return (isSimple_ && isClosed_);
	}

	/*!
	 * @brief Duplicate with ID
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param id SRID
	 *
	 * @return newly allocated LineString
	 */
	LineString *dup(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, srid_t id) {
		if (isEmpty()) {
			return QP_NEW LineString(txn);
		}
		else {
			LineString *ls = QP_NEW LineString(id, p_, txn, objectManager, strategy);
			ls->isAssigned_ = isAssigned_;
			ls->isEmpty_ = isEmpty_;
			return ls;
		}
	}

	/*!
	 * @brief  Duplicate
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return Duplicated LineString
	 */
	LineString *dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		return dup(txn, objectManager, strategy, srId_);
	}

	/*!
	 * @brief Get geometry type
	 *
	 */
	virtual GeometryType getType() const {
		return LINESTRING;
	}

	/*!
	 * @brief Get the in-memory size of geometry
	 *
	 * @return type
	 */
	size_t getRawSize() const {
		return sizeof(*this);
	}

	/*!
	 * @brief Cast to string
	 *
	 */
	virtual const char *getString(TransactionContext &txn) const {
		util::NormalOStringStream os;
		os << "LINESTRING(";
		stringify(txn, os);
		os << ')';
		std::string str = os.str();
		size_t len = str.size();
		char *ret = static_cast<char *>(QP_ALLOCATOR.allocate(len + 1));
		memcpy(ret, str.c_str(), len);
		ret[len] = '\0';
		return ret;
	}

	/*!
	 * @brief Check for object equality
	 *
	 * @param line LineString
	 *
	 * @return result
	 */
	bool operator==(LineString &line) {
		if (p_.size() != line.p_.size()) {
			return false;
		}
		QP_XArray<Point *>::const_iterator it1 = p_.begin();
		QP_XArray<Point *>::const_iterator it2 = line.p_.begin();
		for (; it1 != p_.end(); ++it1) {
			if (**it1 != **it2) {
				return false;
			}
			++it2;
		}
		return true;
	}

	/*!
	 * @brief Concatinate two linestrings
	 *
	 * @return New concatinated linestring
	 */
	LineString *concat(LineString *) {
		return NULL;
	}

	/*!
	 * @brief Calculate the length of the linestring
	 *
	 * @return length
	 */
	double length() const {
		QP_XArray<Point *>::const_iterator it = p_.begin();
		double r = 0;
		if (p_.size() == 0) return 0;
		Point *back = (*p_.begin());
		for (it++; it != p_.end(); ++it) {
			r += sqrt(pow(fabs((*it)->x() - back->x()), 2.0) +
					  pow(fabs((*it)->y() - back->y()), 2.0) +
					  pow(fabs((*it)->z() - back->z()), 2.0));
			back = *it;
		}
		return r;
	}

	/*!
	 * @brief Check for equality
	 *
	 * @return result
	 */
	virtual bool operator==(Geometry &) const {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
			"Internal logic error: not implemented");
	}

protected:
	/*!
	 * @brief Calculate bounding rect and simplicity.
	 *
	 */
	void calcBoundingRectAndSimplicity() {
		if (p_.empty()) {
			boundingRect_.xmin = boundingRect_.ymin = boundingRect_.zmin =
				boundingRect_.xmax = boundingRect_.ymax = boundingRect_.zmax =
					std::numeric_limits<double>::quiet_NaN();
			return;
		}
		size_t i;
		boundingRect_.xmin = p_[0]->x();
		boundingRect_.ymin = p_[0]->y();
		boundingRect_.zmin = p_[0]->z();
		boundingRect_.xmax = p_[0]->x();
		boundingRect_.ymax = p_[0]->y();
		boundingRect_.zmax = p_[0]->z();
		for (i = 1; i < p_.size(); i++) {
			boundingRect_.xmin = std::min(boundingRect_.xmin, p_[i]->x());
			boundingRect_.ymin = std::min(boundingRect_.ymin, p_[i]->y());
			boundingRect_.xmax = std::max(boundingRect_.xmax, p_[i]->x());
			boundingRect_.ymax = std::max(boundingRect_.ymax, p_[i]->y());
			if (dimension_ == 2) {
				boundingRect_.zmin = -std::numeric_limits<double>::infinity();
				boundingRect_.zmax = std::numeric_limits<double>::infinity();
			}
			else {
				boundingRect_.zmin = std::min(boundingRect_.zmin, p_[i]->z());
				boundingRect_.zmax = std::max(boundingRect_.zmax, p_[i]->z());
			}
			for (size_t k = 1; isSimple_ && k < i; k++) {
				isSimple_ = (*p_[k] != *p_[i]);
				if (!isSimple_) {
					break;
				}
			}
		}
		for (size_t k = 1; isSimple_ && k < p_.size() - 1; k++) {
			isSimple_ = (*p_[0] != *p_[k]);
		}
	}

	/*!
	 * @brief Insert a new point into linestring.
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param p point (to be duplicated)
	 * @param n index
	 */
	void insert(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, Point &p,
		int n) {
		if (static_cast<size_t>(n) >= p_.size() || n < 0) {
			isClosed_ = (p == *p_[0]);
			p_.push_back(p.dup(txn, objectManager, strategy));
		}
		else {
			p_.insert(p_.begin() + n, p.dup(txn, objectManager, strategy));
		}
		if (isAssigned() && p.isAssigned()) {
			calcBoundingRectAndSimplicity();
		}
		else {
			isAssigned_ = false;
		}
	}

	virtual size_t getSerializedSize() const {
		return this->PointGeom::getSerializedSize();
	}
	void serializeObject(util::XArray<uint8_t> &out, uint64_t &offset) {
		this->PointGeom::serializeObject(out, offset);
	}

	static LineString *deserializeObject(TransactionContext &txn,
		const uint8_t *in, uint64_t &offset, srid_t srId, int dimension) {
		LineString *p = QP_NEW LineString(txn);
		uint16_t len;
		assignFromMemory(&len, in + offset);
		offset += sizeof(uint16_t);
		p->isSimple_ = true;
		p->isEmpty_ = false;
		p->isAssigned_ = true;
		p->dimension_ = dimension;
		for (uint16_t i = 0; i < len; i++) {
			p->p_.push_back(
				Point::deserializeObject(txn, in, offset, srId, dimension));
		}
		if (p->p_.size() < 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_MAKE_OBJECT,
				"Cannot create linestring from one point.");
		}
		p->isClosed_ = (*p->p_[p->p_.size() - 1] == *p->p_[0]);
		p->isEmpty_ = false;
		if (p->isAssigned_) {
			p->calcBoundingRectAndSimplicity();
		}

		return p;
	}

	static LineString *deserializeObject(TransactionContext &txn,
		util::ByteStream<util::ArrayInStream> &in, srid_t srId, int dimension) {
		LineString *p = QP_NEW LineString(txn);
		uint16_t len;
		in >> len;
		p->isSimple_ = true;
		p->isEmpty_ = false;
		p->isAssigned_ = true;
		p->dimension_ = dimension;
		for (uint16_t i = 0; i < len; i++) {
			p->p_.push_back(Point::deserializeObject(txn, in, srId, dimension));
		}
		if (p->p_.size() < 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_MAKE_OBJECT,
				"Cannot create linestring from one point.");
		}
		p->isClosed_ = (*p->p_[p->p_.size() - 1] == *p->p_[0]);
		p->isEmpty_ = false;
		if (p->isAssigned_) {
			p->calcBoundingRectAndSimplicity();
		}
		return p;
	}
};

#endif /* GIS_LINESTRING_HPP */
