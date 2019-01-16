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
	@brief Base class for 3D-polyhedralsurface data
*/

#ifndef GIS_POLYHEDRALSURFACE_HPP
#define GIS_POLYHEDRALSURFACE_HPP

#include <sstream>

#include "util/container.h"

#include "expression.h"
#include "gis_geometry.h"
#include "gis_multipolygon.h"
#include "gis_point.h"
#include "gis_pointgeom.h"
#include "gis_polygon.h"

/*!
 * @brief PolyhedralSurface class
 *
 */
class PolyhedralSurface : public MultiPolygon {
	friend class Geometry;

public:
	/*!
	 * @brief Constructor
	 * @param txn The transaction context
	 */
	explicit PolyhedralSurface(TransactionContext &txn) : MultiPolygon(txn) {}

	/*!
	 * @brief Destructor
	 */
	~PolyhedralSurface() {}

	/*!
	 * @brief Constructor from Polygon array
	 *
	 * @param id SRID
	 * @param parray Source polygon array
	 * @param txn The transaction context
	 * @param txn Object manager
	 */
	PolyhedralSurface(srid_t id, const QP_XArray<Polygon *> &parray,
		TransactionContext &txn, ObjectManager &objectManager)
		: MultiPolygon(id, parray, txn, objectManager) {}
	virtual GeometryType getType() const {
		return POLYHEDRALSURFACE;
	}

	/*!
	 * Get the in-memory size of geometry
	 *
	 * @return type
	 */
	size_t getRawSize() const {
		return sizeof(*this);
	}

	/*!
	 * @brief Constructor from Multipolygon
	 *
	 * @param id SRID
	 * @param base Multipolygon object
	 * @param txn The transaction context
	 * @param txn Object manager
	 */
	PolyhedralSurface(srid_t id, MultiPolygon &base, TransactionContext &txn,
		ObjectManager &objectManager)
		: MultiPolygon(id, base.polygonArray_, txn, objectManager) {}

	/*!
	 * @brief Duplication
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param id SRID
	 *
	 * @return Generated geometry object
	 */
	PolyhedralSurface *dup(
		TransactionContext &txn, ObjectManager &objectManager, srid_t id) {
		if (isEmpty()) {
			return QP_NEW PolyhedralSurface(txn);
		}
		else {
			PolyhedralSurface *ps =
				QP_NEW PolyhedralSurface(id, polygonArray_, txn, objectManager);
			ps->isAssigned_ = isAssigned_;
			ps->isEmpty_ = isEmpty_;
			return ps;
		}
	}

	/*!
	 * @brief Duplication
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 * @return Generated geometry object
	 */
	PolyhedralSurface *dup(
		TransactionContext &txn, ObjectManager &objectManager) {
		return dup(txn, objectManager, srId_);
	}

	/*!
	 * @brief Stringify the geometry
	 * @param txn Transaction context
	 * @return WKT string
	 */
	virtual const char *getString(TransactionContext &txn) const {
		util::NormalOStringStream os;
		os << "POLYHEDRALSURFACE(";
		stringify(txn, os);
		os << ')';
		std::string str = os.str();
		const size_t len = str.size();
		char *ret = static_cast<char *>(QP_ALLOCATOR.allocate(len + 1));
		memcpy(ret, str.c_str(), len);
		ret[len] = '\0';
		return ret;
	}

	using MultiPolygon::operator==;
	/*!
	 * @brief Cannot test the equality
	 * @return None. always throws exception.
	 */
	virtual bool operator==(PolyhedralSurface &) const {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_NOT_IMPLEMENTED,
			"Internal logic error: not implemented");
	}

	/*!
	 * @brief Cannot test the equality
	 * @return False if type is not POLYHEDRALSURFACE. otherwise throws
	 * exception.
	 */
	virtual bool operator==(Geometry &g) const {
		GeometryType t = g.getType();
		if (t != POLYHEDRALSURFACE &&
			!(t == POLYGON && polygonArray_.size() == 1)) {
			return false;
		}
		return *this == dynamic_cast<PolyhedralSurface &>(g);
	}

	/*!
	 * @brief Check the simplicity
	 * @return result
	 */
	bool isSimple() const {
		return isSimple_;
	}

protected:
	/*!
	 * @brief Calculate serialized size of the geometry
	 * @return result
	 */
	virtual size_t getSerializedSize() const {
		return this->MultiPolygon::getSerializedSize();
	}

	/*!
	 * @brief Serialize the object into output buffer
	 *
	 * @param out The output buffer
	 * @param offset Write pointer
	 */
	void serializeObject(util::XArray<uint8_t> &out, uint64_t &offset) {
		this->MultiPolygon::serializeObject(out, offset);
	}

	/*!
	 * @brief Deserialize object from byte array
	 *
	 * @param txn Transaction Context
	 * @param in Input byte array
	 * @param offset Read pointer
	 * @param srId SRID
	 * @param dimension Passed from Geometry::deserialize
	 *
	 * @return Generated object
	 */
	static PolyhedralSurface *deserializeObject(TransactionContext &txn,
		const uint8_t *in, uint64_t &offset, int srId, int dimension) {
		PolyhedralSurface *p = QP_NEW PolyhedralSurface(txn);
		uint16_t nPolygon;

		assignFromMemory(&nPolygon, in + offset);
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

	/*!
	 * @brief Deserialize object from byte stream
	 *
	 * @param txn Transaction Context
	 * @param in Input byte stream
	 * @param srId SRID
	 * @param dimension Passed from Geometry::deserialize
	 *
	 * @return Generated object
	 */
	static PolyhedralSurface *deserializeObject(TransactionContext &txn,
		util::ByteStream<util::ArrayInStream> &in, int srId, int dimension) {
		PolyhedralSurface *p = QP_NEW PolyhedralSurface(txn);
		uint16_t nPolygon;
		in >> nPolygon;

		p->dimension_ = dimension;
		for (uint16_t i = 0; i < nPolygon; i++) {
			p->polygonArray_.push_back(
				Polygon::deserializeObject(txn, in, srId, dimension));
		}
		p->calcBoundingRectAndSimplicity();
		p->isEmpty_ = (dimension == 0) || (nPolygon == 0);
		return p;
	}
};

#endif /* GIS_POLYHEDRALSURFACE_HPP */
