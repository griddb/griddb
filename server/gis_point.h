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
	@brief Point class for 3D-geometry data
*/

#ifndef GIS_POINT_H_
#define GIS_POINT_H_

#include "util/container.h"
#include <cfloat>
#include <iomanip>
#include <limits>
#include <sstream>

#include "expression.h"
#include "gis_geometry.h"
#define GIS_POINT_PRECISION 16

class PointGeom;
class LineString;

/*!
 * @brief expresses a 2D or 3D Point
 *
 */
class Point : public Geometry {
	friend class LineString;
	friend class PointGeom;
	friend class Geometry;

public:
	/*!
	 * @brief Constructor for empty objects
	 * @param txn The transaction context
	 */
	explicit Point(TransactionContext &txn) : Geometry(txn) {
		isEmpty_ = true;
		isAssigned_ = true;
		xExpr_ = NULL;
		yExpr_ = NULL;
		zExpr_ = NULL;
		srId_ = -1;
		x_ = y_ = z_ = std::numeric_limits<double>::quiet_NaN();
		dimension_ = 0;
	}

	/*!
	 * @brief Constructor from xyz-coordinate
	 *
	 * @param id SRID
	 * @param x X-coordinate
	 * @param y Y-coordinate
	 * @param z Z-coordinate
	 * @param txn The transaction context
	 *
	 */
	Point(srid_t id, double x, double y, double z, TransactionContext &txn)
		: Geometry(txn) {
		isEmpty_ = false;
		isAssigned_ = true;
		x_ = x;
		boundingRect_.xmax = boundingRect_.xmin = x_;
		y_ = y;
		boundingRect_.ymax = boundingRect_.ymin = y_;
		z_ = z;
		if (!util::isNaN(z)) {
			boundingRect_.zmax = boundingRect_.zmin = z_;
			dimension_ = 3;
		}
		else {
			boundingRect_.zmax = std::numeric_limits<double>::infinity();
			boundingRect_.zmin = -std::numeric_limits<double>::infinity();
			dimension_ = 2;
		}
		srId_ = id;
		xExpr_ = NULL;
		yExpr_ = NULL;
		zExpr_ = NULL;
	}

	/*!
	 * @brief Construct an non-deterministic point object.
	 *
	 * @param id SRID
	 * @param x Expression treated as X
	 * @param y Expression treated as Y
	 * @param z Expression treated as Z
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 */
	Point(srid_t id, Expr *x, Expr *y, Expr *z, TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy)
		: Geometry(txn) {
		isEmpty_ = (x == NULL) && (y == NULL) && (z == NULL);
		srId_ = id;
		isAssigned_ = true;

		xExpr_ = NULL;
		if (x != NULL && x->isNumeric()) {
			x_ = *x;
		}
		else if (x != NULL) {
			x_ = std::numeric_limits<double>::quiet_NaN();
			xExpr_ = x->dup(txn, objectManager, strategy);
			isAssigned_ = false;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_LOGIC_ERROR,
				"Internal logic error: Point constructed with no expr");
		}

		yExpr_ = NULL;
		if (y != NULL && y->isNumeric()) {
			y_ = *y;
		}
		else if (y != NULL) {
			y_ = std::numeric_limits<double>::quiet_NaN();
			yExpr_ = y->dup(txn, objectManager, strategy);
			isAssigned_ = false;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_LOGIC_ERROR,
				"Internal logic error: Point constructed with no expr");
		}
		dimension_ = 2;

		zExpr_ = NULL;
		if (z != NULL && z->isNumeric()) {
			z_ = *z;
			dimension_ = 3;
		}
		else {
			z_ = std::numeric_limits<double>::quiet_NaN();
			if (z != NULL) {
				zExpr_ = z->dup(txn, objectManager, strategy);
				dimension_ = 3;
			}
			isAssigned_ = (z == NULL);
		}

		if (isAssigned_) {
			boundingRect_.xmax = boundingRect_.xmin = x_;
			boundingRect_.ymax = boundingRect_.ymin = y_;
			boundingRect_.zmax = boundingRect_.zmin = z_;
		}
		else {
			boundingRect_.xmax = boundingRect_.xmin = boundingRect_.ymax =
				boundingRect_.ymin = boundingRect_.zmax = boundingRect_.zmin =
					std::numeric_limits<double>::quiet_NaN();
		}
	}


	/*!
	 * @brief Check for the object is deterministic object
	 *
	 */
	bool isAssigned() const {
		return isAssigned_;
	}

	/*!
	 * @brief Generate new assigned point object
	 * ex) assign POINT(A 1 2) to A=0 -> new POINT(0 1 2) is generated.
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param amap Symbol map to assign used in expression evaluation
	 * @param fmap Function map to call used in expression evaluation
	 * @param mode EvalMode
	 *
	 * @return newly generated point object
	 */
	virtual Point *assign(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		ContainerRowWrapper *amap, FunctionMap *fmap, EvalMode mode) {
		if (isEmpty_ || isAssigned_) {
			return dup(txn, objectManager, strategy);
		}

		Expr *e1 = NULL, *e2 = NULL, *e3 = NULL;
		e1 = (xExpr_ != NULL)
				 ? (xExpr_->eval(txn, objectManager, strategy, amap, fmap, mode))
				 : (Expr::newNumericValue(x_, txn));
		e2 = (yExpr_ != NULL)
				 ? (yExpr_->eval(txn, objectManager, strategy, amap, fmap, mode))
				 : (Expr::newNumericValue(y_, txn));
		if (dimension_ >= 3) {
			e3 = (zExpr_ != NULL)
					 ? (zExpr_->eval(txn, objectManager, strategy, amap, fmap, mode))
					 : (Expr::newNumericValue(z_, txn));
		}
		return QP_NEW Point(srId_, e1, e2, e3, txn, objectManager, strategy);
	}

	/*!
	 * @brief Check for the object is simple
	 *
	 * @return result
	 */
	bool isSimple() const {
		return isAssigned_;
	}

	/*!
	 * @brief Get type
	 *
	 * @return type
	 */
	GeometryType getType() const {
		return POINT;
	}

	virtual size_t getRawSize() const {
		return sizeof(*this);
	}

	/*!
	 * @brief Get MBB (Mininum-Bounding-Box) rectangle (just point).
	 *
	 * @return MBB rect
	 */
	const TrRectTag &getBoundingRect() const {
		return boundingRect_;
	}

	/*!
	 * @brief Generate duplication of the object
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @param id SRID
	 *
	 * @return duplicated object, caller must release.
	 */
	Point *dup(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, srid_t id) {
		if (isEmpty()) {
			return QP_NEW Point(txn);
		}
		else {
			Point *p = QP_NEW Point(id, x_, y_, z_, txn);
			if (xExpr_ != NULL) {
				p->xExpr_ = xExpr_->dup(txn, objectManager, strategy);
			}
			if (yExpr_ != NULL) {
				p->yExpr_ = yExpr_->dup(txn, objectManager, strategy);
			}
			if (zExpr_ != NULL) {
				p->zExpr_ = zExpr_->dup(txn, objectManager, strategy);
			}
			p->isAssigned_ = isAssigned_;
			p->isEmpty_ = isEmpty_;
			return p;
		}
	}
	/*!
	 * @brief Duplicate object
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 * @return duplicated object, caller must release
	 */
	Point *dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		return dup(txn, objectManager, strategy, srId_);
	}

	/*!
	 * @brief Get X coordinate
	 *
	 * @return x-coordinate if possible to evaluate.
	 */
	double x() const {
		if (isEmpty_) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_INTERNAL_GIS_GET_VALUE_IN_EMPTY_OBJECT,
				"Cannot obtain coordinate from empty object");
		}
		else if (isAssigned_ || xExpr_ == NULL) {
			return x_;
		}
		else if (!isAssigned_ && xExpr_->isNumeric()) {
			return double(*xExpr_);
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_LOGIC_ERROR,
			"Internal logic error: cannot obtain undetermined coordinate");
	}

	/*!
	 * @brief Get Y coordinate
	 *
	 * @return y-coordinate if possible to evaluate.
	 */
	double y() const {
		if (isEmpty_) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_INTERNAL_GIS_GET_VALUE_IN_EMPTY_OBJECT,
				"Cannot obtain coordinate from empty object");
		}
		else if (isAssigned_ || yExpr_ == NULL) {
			return y_;
		}
		else if (!isAssigned_ && yExpr_->isNumeric()) {
			return double(*yExpr_);
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_LOGIC_ERROR,
			"Internal logic error: cannot obtain undetermined coordinate");
	}

	/*!
	 * @brief Get Z coordinate
	 *
	 * @return z-coordinate if possible to evaluate.
	 */
	double z() const {
		if (isEmpty_) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_INTERNAL_GIS_GET_VALUE_IN_EMPTY_OBJECT,
				"Cannot obtain coordinate from empty object");
		}
		else if (dimension_ >= 3 && (isAssigned_ || zExpr_ == NULL)) {
			return z_;
		}
		else if (dimension_ >= 3 && !isAssigned_ && zExpr_->isNumeric()) {
			return double(*zExpr_);
		}
		else if (dimension_ == 2) {
			return z_;
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_LOGIC_ERROR,
			"Internal logic error: cannot obtain undetermined coordinate");
	}

	/*!
	 * @brief Get WKT string of the object
	 *
	 * @return WKT
	 */
	virtual const char *getString(TransactionContext &txn) const {
		util::NormalOStringStream os;
		os << "POINT(";
		stringify(txn, os);
		os << ')';
		std::string str = os.str();
		const size_t len = str.size();
		char *ret = static_cast<char *>(QP_ALLOCATOR.allocate(len + 1));
		memcpy(ret, str.c_str(), len);
		ret[len] = '\0';
		return ret;
	}

#ifdef _WIN32
	/*!
	 * @brief Stringify a double value
	 *
	 * @param os output string stream
	 * @param x value
	 */
	inline void stringifyFp(util::NormalOStringStream &os, double x) const {
		if (util::isNaN(x)) {
			os << "nan";
		}
		else if (x == std::numeric_limits<double>::infinity()) {
			os << "inf";
		}
		else if (x == -std::numeric_limits<double>::infinity()) {
			os << "-inf";
		}
		else {
			os << x;
		}
	}
#else
	/*!
	 * @brief Stringify a double value
	 *
	 * @param os output string stream
	 * @param x value
	 */
	inline void stringifyFp(util::NormalOStringStream &os, double x) const {
		os << x;
	}
#endif

	/*!
	 * @brief Get object coordinate in WKT-subset string
	 *
	 * @param txn The transaction context
	 * @param os string-stream to save
	 */
	virtual void stringify(
		TransactionContext &txn, util::NormalOStringStream &os) const {
		std::streamsize precision = os.precision();
		os << std::setprecision(GIS_POINT_PRECISION);
		if (isEmpty_) {
			os << "EMPTY";
		}
		else if (isAssigned_) {
			stringifyFp(os, x_);
			os << ' ';
			stringifyFp(os, y_);
			if (dimension_ >= 3) {
				os << ' ';
				stringifyFp(os, z_);
			}
		}
		else {
			if (xExpr_ != NULL) {
				os << xExpr_->getValueAsString(txn);
			}
			else {
				os << x_;
			}
			os << ' ';
			if (yExpr_ != NULL) {
				os << yExpr_->getValueAsString(txn);
			}
			else {
				os << y_;
			}
			if (dimension_ >= 3) {
				os << ' ';
				if (zExpr_ != NULL) {
					os << zExpr_->getValueAsString(txn);
				}
				else {
					os << z_;
				}
			}
		}
		if (srId_ != -1) {
			os << ";" << srId_;
		}
		os << std::setprecision(static_cast<int32_t>(precision));
	}

	/*!
	 * @brief Check for equality
	 *
	 * @param p point to test
	 *
	 * @return result
	 */
	bool operator==(Point &p) const {
		if (isEmpty_) {
			return p.isEmpty();
		}
		if (p.isEmpty_) {
			return false;
		}
		if (dimension_ == 2) {
			return (x_ == p.x() && y_ == p.y());
		}
		return (x_ == p.x() && y_ == p.y() && z_ == p.z());
	}

	/*!
	 * @brief Check for equality
	 *
	 * @param g geometry to test
	 *
	 * @return result
	 */
	bool operator==(Geometry &g) const {
		if (g.getType() != POINT) {
			return false;
		}
		return *this == dynamic_cast<Point &>(g);
	}

	/*!
	 * @brief Check for non-equality
	 *
	 * @param p point to test
	 *
	 * @return
	 */
	bool operator!=(Point &p) const {
		return !(*this == p);
	}

protected:
	double x_, y_, z_;
	Expr *xExpr_, *yExpr_, *zExpr_;

	/*!
	 * @brief Get the serialized size of geometry body
	 * (Not equal to "sizeof")
	 *
	 * @return size
	 */
	virtual size_t getSerializedSize() const {
		return sizeof(double) * 3;
	}

	/*!
	 * @brief Serialization of geometry's body
	 *
	 * @param out output buffer
	 * @param offset write pointer
	 */
	void serializeObject(util::XArray<uint8_t> &out, uint64_t &offset) {
		if (isAssigned_) {
			if (dimension_ != 0) {
				out.push_back(reinterpret_cast<uint8_t *>(&x_), sizeof(double));
				out.push_back(reinterpret_cast<uint8_t *>(&y_), sizeof(double));
				out.push_back(reinterpret_cast<uint8_t *>(&z_), sizeof(double));
				offset += getSerializedSize();
			}
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_SERIALIZE,
				"Cannot serialize geometry object");
		}
	}

	/*!
	 * @brief Deserialize geometry from byte array
	 *
	 * @param txn Transaction context
	 * @param in Input byte array
	 * @param offset Read pointer
	 * @param srId SRID if set
	 * @param dimension Dimension read in Geometry::deserialize
	 *
	 * @return Generated geometry object
	 */
	static Point *deserializeObject(TransactionContext &txn, const uint8_t *in,
		uint64_t &offset, srid_t srId, int dimension) {
		double p[3];
		assignFromMemory(&p[0], in + offset);
		offset += sizeof(double);
		assignFromMemory(&p[1], in + offset);
		offset += sizeof(double);
		assignFromMemory(&p[2], in + offset);
		offset += sizeof(double);

		if (dimension == 2) {
			return QP_NEW Point(srId, p[0], p[1],
				std::numeric_limits<double>::quiet_NaN(), txn);
		}
		else {
			if (dimension != 3) {
				GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_CANNOT_DESERIALIZE,
					"Cannot use dimension >3");
			}
			return QP_NEW Point(srId, p[0], p[1], p[2], txn);
		}
	}

	/*!
	 * @brief Deserialize geometry from byte stream
	 *
	 * @param txn Transaction context
	 * @param in Input byte stream
	 * @param srId SRID if set
	 * @param dimension Dimension read in Geometry::deserialize
	 *
	 * @return Generated geometry object
	 */
	static Point *deserializeObject(TransactionContext &txn,
		util::ByteStream<util::ArrayInStream> &in, srid_t srId, int dimension) {
		double p[3];
		in >> p[0] >> p[1] >> p[2];

		if (dimension == 2) {
			return QP_NEW Point(srId, p[0], p[1],
				std::numeric_limits<double>::quiet_NaN(), txn);
		}
		else if (dimension == 3) {
			return QP_NEW Point(srId, p[0], p[1], p[2], txn);
		}
		util::NormalOStringStream os;
		os << "Invalid dimension: " << dimension;
		GS_THROW_USER_ERROR(
			GS_ERROR_TQ_INTERNAL_GIS_CANNOT_DESERIALIZE, os.str().c_str());
	}
};

#endif /* GIS_POINT_H_*/
