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
	@brief Base class for 3D-quadraticsurface data
*/

#ifndef GIS_QUADRATICSURFACE_H_
#define GIS_QUADRATICSURFACE_H_

#include "util/type.h"
#include "TrPv3.h"
#include "TrTree.h"
#include "gis_geometry.h"
#include "qp_def.h"
#include "transaction_context.h"
#include <cstdarg>

/*!
 * @brief Quadraticsurface class
 *
 */
class QuadraticSurface : public Geometry {
public:
	friend class Geometry;
	friend class QuadraticSurfaceGenerator;

	/*!
	 * @brief Constructor
	 *
	 * @param txn The transaction context
	 * @param type Type of Quadratic Surface
	 * @param num Number of argument list (va_list)
	 *
	 */
	QuadraticSurface(TransactionContext &txn, TrPv3Key_type type, int num, ...)
		: Geometry(txn) {
		va_list va;
		dimension_ = 3;
		va_start(va, num);
		memset(&pkey_, 0, sizeof(pkey_));
		pkey_.type = type;
		if (num == -1) {
			pkey_.A[0][0] = va_arg(va, double);
			pkey_.A[0][1] = va_arg(va, double);
			pkey_.A[0][2] = va_arg(va, double);
			pkey_.A[1][0] = va_arg(va, double);
			pkey_.A[1][1] = va_arg(va, double);
			pkey_.A[1][2] = va_arg(va, double);
			pkey_.A[2][0] = va_arg(va, double);
			pkey_.A[2][1] = va_arg(va, double);
			pkey_.A[2][2] = va_arg(va, double);
			pkey_.b[0] = va_arg(va, double);
			pkey_.b[1] = va_arg(va, double);
			pkey_.b[2] = va_arg(va, double);
			pkey_.c = va_arg(va, double);
			pkey_.p[0] = va_arg(va, double);
			pkey_.p[1] = va_arg(va, double);
			pkey_.p[2] = va_arg(va, double);
			pkey_.negative = 0;
		}
		else {
			switch (type) {
			case TR_PV3KEY_NONE: {
				if (num != 13 && num != -1) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
						"Invalid argument count");
				}
				pkey_.A[0][0] = va_arg(va, double);
				pkey_.A[0][1] = va_arg(va, double);
				pkey_.A[0][2] = va_arg(va, double);
				pkey_.A[1][0] = va_arg(va, double);
				pkey_.A[1][1] = va_arg(va, double);
				pkey_.A[1][2] = va_arg(va, double);
				pkey_.A[2][0] = va_arg(va, double);
				pkey_.A[2][1] = va_arg(va, double);
				pkey_.A[2][2] = va_arg(va, double);
				pkey_.b[0] = va_arg(va, double);
				pkey_.b[1] = va_arg(va, double);
				pkey_.b[2] = va_arg(va, double);
				pkey_.c = va_arg(va, double);
				pkey_.p[0] = 0.0;
				pkey_.p[1] = 0.0;
				pkey_.p[2] = 0.0;
				pkey_.negative = 0;
				break;
			}
			case TR_PV3KEY_PLANE: {
				double p0[3], p1[3];
				if (num != 6) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
						"Invalid argument count");
				}
				p0[0] = va_arg(va, double);
				p0[1] = va_arg(va, double);
				p0[2] = va_arg(va, double);
				p1[0] = va_arg(va, double);
				p1[1] = va_arg(va, double);
				p1[2] = va_arg(va, double);

				TrPv3Key_plane(&pkey_, p0, p1);
				break;
			}
			case TR_PV3KEY_SPHERE: {
				double p0[3], r;
				if (num != 4) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
						"Invalid argument count");
				}
				p0[0] = va_arg(va, double);
				p0[1] = va_arg(va, double);
				p0[2] = va_arg(va, double);
				r = va_arg(va, double);
				if (r <= 0) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
						"Radius must be positive number");
				}
				TrPv3Key_sphere(&pkey_, p0, r);
				break;
			}
			case TR_PV3KEY_CYLINDER: {
				double p0[3], p1[3], r;
				if (num != 7) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
						"Invalid argument count");
				}
				p0[0] = va_arg(va, double);
				p0[1] = va_arg(va, double);
				p0[2] = va_arg(va, double);
				p1[0] = va_arg(va, double);
				p1[1] = va_arg(va, double);
				p1[2] = va_arg(va, double);
				r = va_arg(va, double);
				TrPv3Key_cylinder(&pkey_, p0, p1, r);
				break;
			}
			case TR_PV3KEY_CONE: {
				double p0[3], p1[3], a;
				if (num != 7) {
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
						"Invalid argument count");
				}
				p0[0] = va_arg(va, double);
				p0[1] = va_arg(va, double);
				p0[2] = va_arg(va, double);
				p1[0] = va_arg(va, double);
				p1[1] = va_arg(va, double);
				p1[2] = va_arg(va, double);
				a = va_arg(va, double);
				TrPv3Key_cone(&pkey_, p0, p1, a);
				break;
			}
			default: {
				util::NormalOStringStream os;
				os << "Internal logic error: cannot make qsf of the type "
				   << type;
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_MAKE_OBJECT,
					os.str().c_str());
				break;
			}
			}
		}
		va_end(va);
	}

	/*!
	 * @brief Test the geometry is empty
	 * @return result
	 */
	bool isEmpty() const {
		return dimension_ == 0;
	}

	/*!
	 * @brief Get simplicity of the geometry
	 * @return true (No complex object can be defined)
	 */
	bool isSimple() const {
		return true;
	}

	/*!
	 * @brief Get type of the geometry
	 * @return QUADRATICSURFACE
	 */
	GeometryType getType() const {
		return QUADRATICSURFACE;
	}

	/*!
	 * @brief Getter for pv3key of the QSF
	 * @return pv3key
	 */
	const TrPv3Key &getPv3Key() const {
		return pkey_;
	}

	/*!
	 * @brief Cannot get bounding rect from the QSF
	 * @return  None. Always throws exception.
	 */
	const TrRectTag &getBoundingRect() const {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_GET_VALUE,
			"Cannot get bounding rect from infinite quadractic-surface.");
	}

	/*!
	 * @brief Check the geometry intersection
	 * @param obj Geometry to test
	 * @return result
	 */
	bool isBoundingRectIntersects(const Geometry &obj) const {
		TrRectTag r = obj.getBoundingRect();
		TrPv3Box box;

		box.p0[0] = r.xmin;
		box.p0[1] = r.ymin;
		box.p0[2] = r.zmin;
		box.p1[0] = r.xmax - r.xmin;
		box.p1[1] = r.ymax - r.ymin;
		box.p1[2] = r.zmax - r.zmin;

		return (TrPv3Test2(&box, const_cast<TrPv3Key *>(&pkey_)) != 0);
	}

	/*!
	 * @brief Duplicate the geometry
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 * @return Duplicated geometry
	 * @attention It returns pointer, thus release must be done by the caller
	 */
	Geometry *dup(
		TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &, srid_t) {
		if (isEmpty()) {
			return QP_NEW QuadraticSurface(txn);
		}
		else {
			QuadraticSurface *pQsf = QP_NEW QuadraticSurface(txn, pkey_.type,
				-1, pkey_.A[0][0], pkey_.A[0][1], pkey_.A[0][2], pkey_.A[1][0],
				pkey_.A[1][1], pkey_.A[1][2], pkey_.A[2][0], pkey_.A[2][1],
				pkey_.A[2][2], pkey_.b[0], pkey_.b[1], pkey_.b[2], pkey_.c,
				pkey_.p[0], pkey_.p[1], pkey_.p[2]);
			return pQsf;
		}
	}

	/*!
	 * @brief Duplicate the geometry
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 * @return duplicated geometry
	 */
	Geometry *dup(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		return dup(txn, objectManager, strategy, srId_);
	}

	/*!
	 * @brief Get WKT-like string of the geometry
	 * @param txn The transaction context
	 *
	 * @return A WKT-like string
	 */
	virtual const char *getString(TransactionContext &txn) const {
		util::NormalOStringStream os;
		os << "QUADRATICSURFACE(";
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
	 * @brief Geometry comparison
	 *
	 * @param g target geometry
	 *
	 * @return result
	 */
	bool operator==(Geometry &g) const {
		if (g.getType() != getType()) return false;
		return (memcmp(static_cast<QuadraticSurface *>(&g), this,
					sizeof(*this)) == 0);
	}

	/*!
	 * @brief Get real size of the geometry in memory
	 *
	 * @return result
	 */
	size_t getRawSize() const {
		return sizeof(*this);
	}

	/*!
	 * @brief Check the determinisity of the geometry
	 *
	 * @return All the member values are assigned.
	 */
	bool isAssigned() const {
		return isAssigned_;
	}

	/*!
	 * @brief Assign expr in quadratic surface (Not used)
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 *
	 * @return
	 */
	Geometry *assign(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		ContainerRowWrapper *, FunctionMap *, EvalMode) {
		return dup(txn, objectManager, strategy);
	}

#ifdef _WIN32
	/*!
	 * @brief Stringify a double value
	 *
	 * @param os output string stream
	 * @param x value
	 */
	inline void stringifyFp(util::NormalOStringStream &os, double x) const {
		if (_isnan(x)) {
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
	 * @brief Stringify quadratic surface
	 *
	 * @param os Output string stream
	 */
	void stringify(TransactionContext &, util::NormalOStringStream &os) const {
		if (isEmpty()) {
			os << "EMPTY";
		}
		else {
			os << std::setprecision(GIS_POINT_PRECISION);
			for (int i = 0; i < 3; i++) {
				for (int k = 0; k < 3; k++) {
					stringifyFp(os, pkey_.A[i][k]);
					os << ' ';
				}
			}
			for (int i = 0; i < 3; i++) {
				stringifyFp(os, pkey_.b[i]);
				os << ' ';
			}
			stringifyFp(os, pkey_.c);
		}
	}
	virtual ~QuadraticSurface() {}

protected:
	/*!
	 * @brief Serialize
	 *
	 * @param out Output buffer
	 * @param offset Write pointer
	 */
	void serializeObject(util::XArray<uint8_t> &out, uint64_t &offset) {
		uint16_t tmpType = static_cast<uint16_t>(pkey_.type);
		out.push_back(reinterpret_cast<uint8_t *>(&tmpType), sizeof(uint16_t));
		out.push_back(reinterpret_cast<uint8_t *>(pkey_.A), sizeof(double) * 9);
		out.push_back(reinterpret_cast<uint8_t *>(pkey_.b), sizeof(double) * 3);
		out.push_back(reinterpret_cast<uint8_t *>(&pkey_.c), sizeof(double));
		out.push_back(
			reinterpret_cast<uint8_t *>(&pkey_.p), sizeof(double) * 3);
		offset += sizeof(double) * 16 + sizeof(uint16_t);
	}

	/*!
	 * @brief Get the serialized size of geometry body
	 * (Not equal to "sizeof")
	 *
	 * @return size
	 */
	size_t getSerializedSize() const {
		return sizeof(double) * 16 + sizeof(uint16_t);
	}

	/*!
	 * @brief Deserialize geometry from byte array
	 *
	 * @param txn Transaction context
	 * @param in Input byte array
	 * @param offset Read pointer
	 * @param dimension Dimension read in Geometry::deserialize
	 *
	 * @return Generated geometry object
	 */
	static QuadraticSurface *deserializeObject(TransactionContext &txn,
		const uint8_t *in, uint64_t &offset, int, int dimension) {
		if (dimension != 3) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_MAKE_OBJECT,
				"Dimension value is invalid");
		}

		uint16_t tmpType;
		assignFromMemory(&tmpType, in + offset);
		offset += sizeof(uint16_t);

		double p[16];
		for (int i = 0; i < 16; i++) {
			assignFromMemory(&p[i], in + offset);
			offset += sizeof(double);
		}

		QuadraticSurface *q =
			QP_NEW QuadraticSurface(txn, static_cast<TrPv3Key_type>(tmpType),
				-1, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9],
				p[10], p[11], p[12], p[13], p[14], p[15]);
		return q;
	}

	/*!
	 * @brief Deserialize geometry from byte stream
	 *
	 * @param txn Transaction context
	 * @param in Input byte stream
	 * @param dimension Dimension read in Geometry::deserialize
	 *
	 * @return Generated geometry object
	 */
	static QuadraticSurface *deserializeObject(TransactionContext &txn,
		util::ByteStream<util::ArrayInStream> &in, int, int dimension) {
		if (dimension != 3) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_MAKE_OBJECT,
				"Dimension value is invalid");
		}

		uint16_t tmpType;
		in >> tmpType;

		double p[16];
		for (int i = 0; i < 16; i++) {
			in >> p[i];
		}

		QuadraticSurface *q =
			QP_NEW QuadraticSurface(txn, static_cast<TrPv3Key_type>(tmpType),
				-1, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9],
				p[10], p[11], p[12], p[13], p[14], p[15]);
		return q;
	}

	QuadraticSurface(TransactionContext &txn) : Geometry(txn) {}
	TrPv3Key pkey_;
};

#endif /* GIS_QUADRATICSURFACE_HPP */
