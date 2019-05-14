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

#ifndef GIS_GEOMETRY_H_
#define GIS_GEOMETRY_H_

#include "util/type.h"
#include "util/container.h"
#include "data_store_common.h"
#include "TrTree.h"
#include "qp_def.h"
#include "transaction_context.h"
#include "util/numeric.h"


class FunctionMap;
class Expr;
class Value;
class ContainerRowWrapper;

#define SERIALIZE_ALIGN 4
#define GEOMETRY_HEADER_SIZE (3 + sizeof(Geometry::srid_t))

/*!
 * @brief Geometry class
 */
class Geometry {
public:
	/*!
	 * Type definitions
	 */
	typedef int32_t srid_t;
	/*!
	*	@brief Represents the type of Geometry
	*/
	enum GeometryType {
		POINT,
		LINESTRING,
		LINE,
		LINEARRING,
		POLYGON,
		POINTGEOM,
		MULTIPOINT,
		MULTISTRING,
		MULTIPOLYGON,
		POLYHEDRALSURFACE,
		QUADRATICSURFACE
	};

	/*!
	 * @brief Type getter
	 * @return GeometryType
	 */
	virtual GeometryType getType() const = 0;

	/*!
	 * @brief Get Minimum Bounding Rect (MBR) or MBB (Box)
	 * @return MBR or MBB
	 */
	virtual const TrRectTag &getBoundingRect() const = 0;

	/*!
	 * @brief Get its SRID
	 * @return SRID
	 */
	srid_t getSrId() const {
		return srId_;
	}

	/*!
	 * @brief Set its SRID (without coordinate transformation)
	 * @return SRID
	 */
	void setSrId(srid_t id) {
		srId_ = id;
	}

	/*!
	 * @brief Check the geometry is empty
	 * @return result
	 */
	virtual bool isEmpty() const {
		return isEmpty_;
	}

	/*!
	 * @brief Check the geometry is simple
	 * @return result
	 */
	virtual bool isSimple() const = 0;

	/*!
	 * @brief Check the geometry is not simple
	 * @return result
	 */
	virtual bool isComplex() const {
		return !isSimple();
	}

	/*!
	 * @brief Check the geometry is able to serialize and store to DB
	 * @param type geometry Type
	 * @return result
	 */
	static bool isStorable(GeometryType type) {
		return (type != POINTGEOM) && (type != MULTIPOINT) &&
			   (type != MULTISTRING) && (type != MULTIPOLYGON) &&
			   (type != QUADRATICSURFACE);
	}

	/*!
	 * @brief Check this geometry is able to serialize and store to DB
	 * @return result
	 */
	bool isStorable() const {
		return isStorable(getType());
	}

	/*!
	 * @brief Check this geometry is already evaluated
	 * @return result
	 */
	bool isAssigned() const {
		return isAssigned_;
	}

	/*!
	 * @brief Duplication
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param id SRID if set
	 *
	 * @return Duplicated geometry object
	 */
	virtual Geometry *dup(TransactionContext &txn, ObjectManager &objectManager,
		srid_t id) = 0;  

	/*!
	 * @brief Duplicate with its srId
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return Duplicated geometry object
	 */
	virtual Geometry *dup(TransactionContext &txn,
		ObjectManager &objectManager) = 0;  

	/*!
	 * @brief Get WKT of the geometry
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return WKT
	 */
	virtual const char *getString(TransactionContext &txn) const = 0;

	/*!
	 * @brief Check the equality of geometries
	 *
	 * @param geom Geometry to compare with
	 *
	 * @return True if the two geoms are equal
	 */
	virtual bool operator==(Geometry &geom) const = 0;

	/*!
	 * @brief Get size of the geometry's serialized presentation
	 * (Not equal to the size on memory)
	 *
	 * @return size
	 */
	size_t getSerializedSizeOfBytes() const {
		size_t firstSize = GEOMETRY_HEADER_SIZE + this->getSerializedSize();
		return (firstSize % SERIALIZE_ALIGN)
				   ? (firstSize +
						 (SERIALIZE_ALIGN - firstSize % SERIALIZE_ALIGN))
				   : firstSize;
	}

	/*!
	 * @brief Get memory size of the geometry
	 * (Not equal to the size of serialized presentation)
	 *
	 * @return size
	 */
	virtual size_t getRawSize() const = 0;

	/*!
	 * @brief Assign undetermined values (not used now)
	 *
	 * @param txn
	 * @param column_values
	 * @param fmap
	 * @param mode
	 *
	 * @return
	 */
	virtual Geometry *assign(TransactionContext &txn,
		ObjectManager &objectManager, ContainerRowWrapper *column_values,
		FunctionMap *fmap, EvalMode mode) = 0;
	/*!
	 * @brief Get Sub-WKT expression of the geometry (w/o Type)
	 * @param[in] txn TransactionContext
	 * @param os output stream
	 */
	virtual void stringify(
		TransactionContext &txn, util::NormalOStringStream &os) const = 0;

	/*!
	 * @brief Get dimension of the geometry
	 * @return dimension
	 */
	int getDimension() const {
		return dimension_;
	}

	/*!
	 * @brief Check that its bounding rect touches with the one of another
	 * object.
	 *
	 * @param obj target geometry
	 *
	 * @return result
	 */
	virtual bool isBoundingRectIntersects(const Geometry &obj) const {
		const TrRectTag &a = getBoundingRect(), &b = obj.getBoundingRect();
		if (isEmpty() || obj.isEmpty()) {
			return false;
		}
		/*
		! ((a.xmax < b.xmin) || (a.xmin > b.xmax))
		-> (!(a.xmax < b.xmin) && !(a.xmin > b.xmax))
		-> (a.xmax >= b.xmin) && (a.xmin <= b.xmax)

		!(dimension_>2 && ((a.zmax < b.zmin) || (a.zmin > b.zmax)))
		-> !dimension_>2 || !((a.zmax < b.zmin) || (a.zmin > b.zmax))
		*/
		return (
			(a.xmin <= b.xmax) && (b.xmin <= a.xmax) &&
			((a.ymin <= b.ymax) && (b.ymin <= a.ymax)) &&
			(dimension_ <= 2 || ((a.zmin <= b.zmax) && (b.zmin <= a.zmax))));
	}

	/*!
	 * @brief Check that its bounding rect includes the one of another object.
	 *
	 * @param obj
	 *
	 * @return
	 */
	virtual bool isBoundingRectInclude(const Geometry &obj) const {
		const TrRectTag &out = getBoundingRect(), &in = obj.getBoundingRect();
		if (isEmpty() || obj.isEmpty()) {
			return false;
		}
		return (
			out.xmin <= in.xmin && out.xmax >= in.xmax && out.ymin <= in.ymin &&
			out.ymax >= in.ymax &&
			(dimension_ <= 2 || (out.zmin <= in.zmin && out.zmax >= in.zmax)));
	}

	/*!
	 * @brief Deserialize geometry from binary
	 *
	 * @param txn Transaction context
	 * @param in input byte array
	 * @param size size of the array
	 *
	 * @return Generated geometry
	 */
	static Geometry *deserialize(
		TransactionContext &txn, const uint8_t *in, size_t size);

	/*!
	 * @brief Serialize geometry into binary
	 *
	 * @param geom geometry
	 * @param out output buffer
	 */
	static void serialize(Geometry *geom, util::XArray<uint8_t> &out);

	/*!
	 * @brief Check the geometry is storable to DB
	 *
	 * @param geomArray Serialized byte array
	 * @param geomSize array size
	 *
	 * @return True if the geometry is able to store into DB
	 */
	static bool isStorable(const uint8_t *geomArray, size_t geomSize);

	/* Left for OpenGIS Fullset
   virtual double distance(Geometry* geom);
   virtual bool equals(Geometry* geom);
   virtual bool disjoint(Geometry* geom);
   virtual bool intersects(Geometry* geom);
   virtual bool touches(Geometry* geom);
   virtual bool crosses(Geometry* geom);
   virtual bool within(Geometry* geom);
   virtual bool overlaps(Geometry* geom);
   virtual bool contains(Geometry* geom);
   virtual bool relate(Geometry* geom, const char *intersectionPatternString);

   Geometry* centroid();
   Geometry* area();
   double length();
   Geometry* pointOnSurface();
   Geometry* boundary();
   Geometry* buffer(double fDistance, integer nSegments);
   Geometry* convexHull();
   Geometry* intersection(Geometry *geom);
   Geometry* difference(Geometry *geom);
   Geometry* geomunion(Geometry *geom);
	*/

	/*!
	 * @brief Destructor
	 */
	virtual ~Geometry() {}
	/*! 
	 * @brief Deserialize geometry header from binary
	 * 
	 * @param in input byte array
	 * @param size size of the array
	 * @param type type of geometry
	 * @return True if value is empty, false otherwise;
	 */
	static bool deserializeHeader(const uint8_t *in, size_t size, int16_t &typex) {
		util::ByteStream<util::ArrayInStream> is(util::ArrayInStream(in, size));
		int8_t dimension = 0;
		srid_t srId = -1;

		deserializeHeader(is, typex, srId, dimension);
		if (dimension == 0){
			return true;
		} else {
			return false;
		}
	}

protected:
	/*!
	 * @brief Serialization of geometry's body
	 *
	 * @param out output buffer
	 * @param offset write pointer
	 */
	virtual void serializeObject(
		util::XArray<uint8_t> &out, uint64_t &offset) = 0;

	/*!
	 * @brief Get the serialized size of geometry body
	 * (Not equal to "sizeof")
	 *
	 * @return size
	 */
	virtual size_t getSerializedSize() const = 0;

	/*!
	 * @brief Constructor of empty geometry
	 *
	 */
	Geometry(TransactionContext &txn) {
		dimension_ = 0;
		isEmpty_ = true;
		isAssigned_ = true;
	}

#if defined(_WIN32) || defined(__i386__) || defined(__ia64__)
	/*!
	 * @brief Memory reader
	 *
	 * @param t read object
	 * @param p memory pointer
	 */
	template <typename T>
	static inline void assignFromMemory(T *t, const uint8_t *p) {
		*t = *const_cast<T *>((reinterpret_cast<const T *>(p)));
	}
#else
	/*!
	 * @brief Memory reader
	 *
	 * @param t read object
	 * @param p memory pointer
	 */
	template <typename T>
	static inline void assignFromMemory(T *t, const uint8_t *p) {
		memcpy(t, p, sizeof(T));
	}
#endif

	/*!
	 * @brief Serialize the geometry.
	 *
	 * @param out Output buffer
	 * @param offset Write pointer
	 */
	void serialize(util::XArray<uint8_t> &out, uint64_t &offset) {
		int16_t type = static_cast<int16_t>(this->getType());
		out.push_back(reinterpret_cast<uint8_t *>(&type), 2);
		out.push_back(reinterpret_cast<uint8_t *>(&srId_), sizeof(srid_t));
		out.push_back(reinterpret_cast<uint8_t *>(&dimension_), 1);

		offset += GEOMETRY_HEADER_SIZE;

		this->serializeObject(out, offset);
	}

	/*!
	 * @brief Deserialize from binary
	 *
	 * @param txn Transaction context
	 * @param in Binary of the geometry
	 *
	 * @return Constructed object
	 */
	static Geometry *deserialize(
		TransactionContext &txn, util::ByteStream<util::ArrayInStream> &in);

	/*!
	 * @brief Deprecated... Left just for object-based build
	 */
	static Geometry *deserialize(TransactionContext &txn, const uint8_t *in);
	/*! 
	 * @brief Deserialize geometry header from binary
	 * 
	 * @param in Binary of the geometry
	 * @param type type of geometry
	 * @param srId srId of geometry
	 * @param dimension dimension of geometry
	 */
	static void deserializeHeader(util::ByteStream<util::ArrayInStream> &in,
		int16_t &typex, srid_t &srId, int8_t &dimension) {
		try {
			in >> typex;
			in >> srId;
			in >> dimension;
		}
		catch (const util::UtilityException &e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(
					e, "Geometry data is broken"));
		}
	}

	srid_t srId_;
	TrRectTag boundingRect_;
	bool isEmpty_;
	bool isAssigned_;
	int dimension_;
};

#endif /* GIS_GEOMETRY_HPP */
