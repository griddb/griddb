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
	@brief Base class for 3D-geometry data
*/

#include "gis_geometry.h"
#include "gis_linestring.h"
#include "gis_point.h"
#include "gis_polygon.h"
#include "gis_polyhedralsurface.h"
#include "gis_quadraticsurface.h"


/*!
 * @brief Deserialize binary presentation into memory.
 *
 * @param txn Transaction context
 * @param in Input byte array
 *
 * @return Generated geometry
 */
Geometry *Geometry::deserialize(TransactionContext &txn, const uint8_t *in) {
	uint64_t offset = 0;
	int16_t typex;
	srid_t srId;
	int8_t dimension;

	assignFromMemory(&typex, in + offset);
	offset += sizeof(int16_t);
	assignFromMemory(&srId, in + offset);
	offset += sizeof(srid_t);
	assignFromMemory(&dimension, in + offset);
	offset += sizeof(int8_t);


	switch (typex) {
	case POINT:
		if (dimension > 0) {
			return Point::deserializeObject(txn, in, offset, srId, dimension);
		}
		else if (dimension == 0) {
			return QP_NEW Point(txn);
		}
		break;
	case LINESTRING:
		if (dimension > 0) {
			return LineString::deserializeObject(
				txn, in, offset, srId, dimension);
		}
		else if (dimension == 0) {
			return QP_NEW LineString(txn);
		}
		break;
	case POLYGON:
		if (dimension > 0) {
			return Polygon::deserializeObject(txn, in, offset, srId, dimension);
		}
		else if (dimension == 0) {
			return QP_NEW Polygon(txn);
		}
		break;
	case POLYHEDRALSURFACE:
		if (dimension == 0) {
			return QP_NEW PolyhedralSurface(txn);
		}
		else if (dimension < 3) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_DESERIALIZE,
				"Invalid dimension for polyhedralsurface");
		}
		return PolyhedralSurface::deserializeObject(
			txn, in, offset, srId, dimension);
	case QUADRATICSURFACE:
		if (dimension == 3) {
			return QuadraticSurface::deserializeObject(
				txn, in, offset, srId, dimension);
		}
		else if (dimension == 0) {
			return QP_NEW QuadraticSurface(txn);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_DESERIALIZE,
				"Invalid dimension for quadraticsurface");
		}
		break;
	default:
		break;
	}
	GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_DESERIALIZE,
		"Cannot deserialize as geometry data");
}

/*!
 * @brief Deserialize from stream
 *
 * @param txn TransactionContext
 * @param in ByteStream of ArrayInStream
 *
 * @return Allocated geometry
 */
Geometry *Geometry::deserialize(
	TransactionContext &txn, util::ByteStream<util::ArrayInStream> &in) {
	int16_t typex = POINT;
	srid_t srId = -1;
	int8_t dimension = 0;

	deserializeHeader(in, typex, srId, dimension);

	switch (typex) {
	case POINT:
		if (dimension > 0) {
			return Point::deserializeObject(txn, in, srId, dimension);
		}
		else if (dimension == 0) {
			return QP_NEW Point(txn);
		}
		break;
	case LINESTRING:
		if (dimension > 0) {
			return LineString::deserializeObject(txn, in, srId, dimension);
		}
		else if (dimension == 0) {
			return QP_NEW LineString(txn);
		}
		break;
	case POLYGON:
		if (dimension > 0) {
			return Polygon::deserializeObject(txn, in, srId, dimension);
		}
		else if (dimension == 0) {
			return QP_NEW Polygon(txn);
		}
		break;
	case POLYHEDRALSURFACE:
		if (dimension == 0) {
			return QP_NEW PolyhedralSurface(txn);
		}
		else if (dimension < 3) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_DESERIALIZE,
				"Invalid dimension for polyhedralsurface");
		}
		return PolyhedralSurface::deserializeObject(txn, in, srId, dimension);
	case QUADRATICSURFACE:
		if (dimension == 3) {
			return QuadraticSurface::deserializeObject(
				txn, in, srId, dimension);
		}
		else if (dimension == 0) {
			return QP_NEW QuadraticSurface(txn);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_DESERIALIZE,
				"Invalid dimension for quadraticsurface");
		}
		break;
	default:
		break;
	}
	GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_DESERIALIZE,
		"Cannot deserialize as geometry data");
}

/*!
 * @brief Check geometry array
 *
 * @param geomArray Byte stream of geometry
 * @param geomSize Size of the bytestream
 *
 * @return
 */
bool Geometry::isStorable(const uint8_t *geomArray, size_t geomSize) {
	if (geomSize <= GEOMETRY_HEADER_SIZE) {
		return false;
	}
	Geometry::GeometryType t = static_cast<Geometry::GeometryType>(
		*reinterpret_cast<const int16_t *>(geomArray));
	return Geometry::isStorable(t);
}

/*!
 * @brief Serializer
 *
 * @param geom Geometry
 * @param out Output array
 */
void Geometry::serialize(Geometry *geom, util::XArray<uint8_t> &out) {
	uint64_t offset = 0;
	geom->serialize(out, offset);
}

/*!
 * @brief Deserializer
 *
 * @param in  Input stream
 *
 * @return Generated geometry
 */
Geometry *Geometry::deserialize(
	TransactionContext &txn, const uint8_t *in, size_t size) {
	util::ByteStream<util::ArrayInStream> is(util::ArrayInStream(in, size));
	return Geometry::deserialize(txn, is);
}

