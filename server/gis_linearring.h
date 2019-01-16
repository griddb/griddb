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
	@brief LinearRing class for 3D-geometry data
*/

#ifndef GIS_LINEARRING_H_
#define GIS_LINEARRING_H_

#include "util/container.h"

#include "gis_linestring.h"
#include "gis_pointgeom.h"

/*!
 * @brief LinearRing class : a special case of LineString
 * where the final point equals to the first point.
 *
 */
class LinearRing : public LineString {
public:
	/*!
	 * Constructor
	 *
	 */
	explicit LinearRing(TransactionContext &txn) : LineString(txn) {}

	/*!
	 * @brief Constructor
	 *
	 * @param id SRID
	 * @param parray Point array (to duplicate)
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 */
	LinearRing(srid_t id, const QP_XArray<Point *> &parray,
		TransactionContext &txn, ObjectManager &objectManager)
		: LineString(id, parray, txn, objectManager) {
		if ((isSimple_ && isClosed_) == false) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_MAKE_OBJECT,
				"WKT argument is not a linearring");
		}
	}

	/*!
	 * @brief Generate LinearRing from LineString
	 *
	 * @param id SRID
	 * @param l LineString for base
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 */
	LinearRing(srid_t id, const LineString *l, TransactionContext &txn,
		ObjectManager &objectManager)
		: LineString(id, l->getPoints(), txn, objectManager) {
		isClosed_ = (p_[p_.size() - 1] == p_[0]);
		if ((isSimple_ && isClosed_) == false) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_MAKE_OBJECT,
				"argument is not a linearring");
		}
	}

	/*!
	 * @brief Duplicate the LinearRing
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param id SRID
	 *
	 * @return Duplicated object
	 */
	LinearRing *dup(
		TransactionContext &txn, ObjectManager &objectManager, srid_t id) {
		if (isEmpty()) {
			return QP_NEW LinearRing(txn);
		}
		else {
			LinearRing *r = QP_NEW LinearRing(id, p_, txn, objectManager);
			r->isAssigned_ = isAssigned_;
			r->isEmpty_ = isEmpty_;
			return r;
		}
	}

	/*!
	 * @brief Duplicate
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 * @return Duplicated object
	 */
	LinearRing *dup(TransactionContext &txn, ObjectManager &objectManager) {
		return dup(txn, objectManager, srId_);
	}

	/*!
	 * @brief Get the type of geometry
	 *
	 * @return type
	 */
	GeometryType getType() const {
		return LINEARRING;
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
	 * @param txn TransactionContext
	 *
	 * @return WKT string
	 */
	virtual const char *getString(TransactionContext &txn) const {
		util::NormalOStringStream os;
		os << "LINEARRING";
		stringify(txn, os);
		os << ')';
		std::string str = os.str();
		const size_t len = str.size();
		char *ret = static_cast<char *>(QP_ALLOCATOR.allocate(len + 1));
		memcpy(ret, str.c_str(), len);
		ret[len] = '\0';
		return ret;
	}

	/*!
	 * @brief Check for equality
	 *
	 * @param line target object
	 *
	 * @return result
	 */
	bool operator==(LinearRing &line) {
		if (p_.size() != line.p_.size()) {
			return false;  
		}
		QP_XArray<Point *>::const_iterator it1 = p_.begin();
		QP_XArray<Point *>::const_iterator it2 = line.p_.begin();
		for (; it1 != p_.end(); ++it1) {
			if (*it1 != *it2) {
				return false;
			}
			++it2;
		}
		return true;
	}

	/*!
	 * @brief Check for equality with geometry
	 *
	 * @return result
	 */
	virtual bool operator==(Geometry &) const {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
			"Internal logic error: not implemented");
	}

	virtual size_t getSerializedSize() const {
		return this->PointGeom::getSerializedSize();
	}
};

#endif /* GIS_LINEARRING_HPP */
