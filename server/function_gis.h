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
	@brief TQL Extension for GIS
*/

#ifndef FUNCTIONS_GIS_H_
#define FUNCTIONS_GIS_H_

#include "expression.h"
#include "gis_geometry.h"
#include "gis_linestring.h"
#include "gis_point.h"
#include "gis_polygon.h"
#include "gis_polyhedralsurface.h"
#include "gis_quadraticsurface.h"
#include "qp_def.h"

#define D(arg) arg->getValueAsDouble()


/*!
 * @brief ST_MAKERECT(p0, p1)
 *
 * @return Rectangle as Polygon
 */
class FunctorMakerect : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManager &objectManager) {
		double x1, x2, y1, y2;

		if (args.size() == 2) {
			if (args[0]->isNullValue() || args[1]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[0]->isGeometry()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 1 must be a 2D point");
			}
			else if (!args[1]->isGeometry()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 2 must be a 2D point");
			}
			Geometry *p, *q;
			p = args[0]->getGeometry();
			q = args[1]->getGeometry();
			if (p->getType() != Geometry::POINT || p->getDimension() != 2) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 1 must be a 2D point");
			}
			if (q->getType() != Geometry::POINT || q->getDimension() != 2) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 2 must be a 2D point");
			}
			x1 = dynamic_cast<Point *>(p)->x();
			y1 = dynamic_cast<Point *>(p)->y();
			x2 = dynamic_cast<Point *>(q)->x();
			y2 = dynamic_cast<Point *>(q)->y();
		}
		else if (args.size() == 4) {  
			if (args[0]->isNullValue() || args[1]->isNullValue() || 
				args[2]->isNullValue() || args[3]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[0]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 1 is not an numeric value");
			}
			else if (!args[1]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 2 is not an numeric value");
			}
			else if (!args[2]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 3 is not an numeric value");
			}
			else if (!args[3]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 4 is not an numeric value");
			}
			x1 = D(args[0]);
			y1 = D(args[1]);
			x2 = D(args[2]);
			y2 = D(args[3]);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}

		if (x1 == x2 || y1 == y2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"Cannot make rectangle. Just a line or point.");
		}
		if (!util::isFinite(x1) || !util::isFinite(x2) || !util::isFinite(y1) || !util::isFinite(y2)) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"Invalid value for coordinates");
		}

		const double nan = std::numeric_limits<double>::quiet_NaN();

		Point p1(-1, x1, y1, nan, txn);
		Point p2(-1, x2, y1, nan, txn);
		Point p3(-1, x1, y2, nan, txn);
		Point p4(-1, x2, y2, nan, txn);
		util::XArray<Point *> pls(txn.getDefaultAllocator());
		pls.push_back(&p1);
		pls.push_back(&p2);
		pls.push_back(&p3);
		pls.push_back(&p4);
		pls.push_back(&p1);
		LinearRing ls(-1, pls, txn, objectManager);
		Polygon *polygon = QP_NEW Polygon(-1, &ls, NULL, txn, objectManager);

		return Expr::newGeometryValue(polygon, txn);
	}

	virtual ~FunctorMakerect() {}
};

/*!
 * @brief ST_MAKEBOX(p0, p1)
 *
 * @return Box as PolyhedralSurface
 */
class FunctorMakebox : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManager &objectManager) {
		double x1, y1, z1, x2, y2, z2;
		if (args.size() == 2) {
			if (args[0]->isNullValue() || args[1]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[0]->isGeometry()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 1 must be a 3D point");
			}
			else if (!args[1]->isGeometry()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 2 must be a 3D point");
			}
			Geometry *p, *q;
			p = args[0]->getGeometry();
			q = args[1]->getGeometry();
			if (p->getType() != Geometry::POINT || p->getDimension() != 3) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 1 must be a 3D point");
			}
			if (q->getType() != Geometry::POINT || q->getDimension() != 3) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 2 must be a 3D point");
			}
			x1 = dynamic_cast<Point *>(p)->x();
			y1 = dynamic_cast<Point *>(p)->y();
			z1 = dynamic_cast<Point *>(p)->z();
			x2 = dynamic_cast<Point *>(q)->x();
			y2 = dynamic_cast<Point *>(q)->y();
			z2 = dynamic_cast<Point *>(q)->z();
		}
		else if (args.size() == 6) {  
			if (args[0]->isNullValue() || args[1]->isNullValue() || 
				args[2]->isNullValue() || args[3]->isNullValue() ||
				args[4]->isNullValue() || args[5]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[0]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 1 is not an numeric value");
			}
			else if (!args[1]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 2 is not an numeric value");
			}
			else if (!args[2]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 3 is not an numeric value");
			}
			else if (!args[3]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 4 is not an numeric value");
			}
			else if (!args[4]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 5 is not an numeric value");
			}
			else if (!args[5]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument 6 is not an numeric value");
			}
			x1 = D(args[0]);
			y1 = D(args[1]);
			z1 = D(args[2]);
			x2 = D(args[3]);
			y2 = D(args[4]);
			z2 = D(args[5]);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}

		if (!util::isFinite(x1) || !util::isFinite(x2) || !util::isFinite(y1) || !util::isFinite(y2) ||
			!util::isFinite(z1) || !util::isFinite(z2)) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"Invalid value for coordinates");
		}

		Point p1(-1, x1, y1, z1, txn);
		Point p2(-1, x1, y1, z2, txn);
		Point p3(-1, x1, y2, z1, txn);
		Point p4(-1, x1, y2, z2, txn);
		Point p5(-1, x2, y1, z1, txn);
		Point p6(-1, x2, y1, z2, txn);
		Point p7(-1, x2, y2, z1, txn);
		Point p8(-1, x2, y2, z2, txn);

		util::XArray<Point *> mp1(txn.getDefaultAllocator());
		util::XArray<Point *> mp2(txn.getDefaultAllocator());
		util::XArray<Point *> mp3(txn.getDefaultAllocator());
		util::XArray<Point *> mp4(txn.getDefaultAllocator());
		util::XArray<Point *> mp5(txn.getDefaultAllocator());
		util::XArray<Point *> mp6(txn.getDefaultAllocator());

		mp1.push_back(&p1);
		mp1.push_back(&p2);
		mp1.push_back(&p3);
		mp1.push_back(&p4);
		mp1.push_back(&p1);
		mp2.push_back(&p1);
		mp2.push_back(&p5);
		mp2.push_back(&p6);
		mp2.push_back(&p7);
		mp2.push_back(&p1);
		mp3.push_back(&p2);
		mp3.push_back(&p6);
		mp3.push_back(&p8);
		mp3.push_back(&p4);
		mp3.push_back(&p2);
		mp4.push_back(&p3);
		mp4.push_back(&p7);
		mp4.push_back(&p5);
		mp4.push_back(&p1);
		mp4.push_back(&p3);
		mp5.push_back(&p4);
		mp5.push_back(&p8);
		mp5.push_back(&p7);
		mp5.push_back(&p3);
		mp5.push_back(&p4);
		mp6.push_back(&p5);
		mp6.push_back(&p6);
		mp6.push_back(&p7);
		mp6.push_back(&p8);
		mp6.push_back(&p5);

		util::XArray<MultiPoint *> pmp1(txn.getDefaultAllocator());
		pmp1.push_back(QP_NEW MultiPoint(-1, mp1, txn, objectManager));
		util::XArray<MultiPoint *> pmp2(txn.getDefaultAllocator());
		pmp2.push_back(QP_NEW MultiPoint(-1, mp2, txn, objectManager));
		util::XArray<MultiPoint *> pmp3(txn.getDefaultAllocator());
		pmp3.push_back(QP_NEW MultiPoint(-1, mp3, txn, objectManager));
		util::XArray<MultiPoint *> pmp4(txn.getDefaultAllocator());
		pmp4.push_back(QP_NEW MultiPoint(-1, mp4, txn, objectManager));
		util::XArray<MultiPoint *> pmp5(txn.getDefaultAllocator());
		pmp5.push_back(QP_NEW MultiPoint(-1, mp5, txn, objectManager));
		util::XArray<MultiPoint *> pmp6(txn.getDefaultAllocator());
		pmp6.push_back(QP_NEW MultiPoint(-1, mp6, txn, objectManager));

		Polygon poly1(-1, &pmp1, txn, objectManager);
		Polygon poly2(-1, &pmp2, txn, objectManager);
		Polygon poly3(-1, &pmp3, txn, objectManager);
		Polygon poly4(-1, &pmp4, txn, objectManager);
		Polygon poly5(-1, &pmp5, txn, objectManager);
		Polygon poly6(-1, &pmp6, txn, objectManager);

		util::XArray<Polygon *> mpoly(txn.getDefaultAllocator());
		mpoly.push_back(&poly1);
		mpoly.push_back(&poly2);
		mpoly.push_back(&poly3);
		mpoly.push_back(&poly4);
		mpoly.push_back(&poly5);
		mpoly.push_back(&poly6);
		PolyhedralSurface *ps =
			QP_NEW PolyhedralSurface(-1, mpoly, txn, objectManager);
		return Expr::newGeometryValue(ps, txn);
	}

	virtual ~FunctorMakebox() {}
};

/*!
 * @brief ST_MAKEQSF(A1,A2,A3,A4,A5,A6,A7,A8,A9,b1,b2,b3,c)
 *
 * @return Quadratic Surface
 */
class FunctorMakeqsf : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 13) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		int i = 0;
		for (i = 0; i < 13; i++) {
			if (args[i]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[i]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument contains a non-numeric value");
			}
		}

		QuadraticSurface *pQsf = QP_NEW QuadraticSurface(txn, TR_PV3KEY_NONE,
			13, D(args[0]), D(args[1]), D(args[2]), D(args[3]), D(args[4]),
			D(args[5]), D(args[6]), D(args[7]), D(args[8]), D(args[9]),
			D(args[10]), D(args[11]), D(args[12]));

		return Expr::newGeometryValue(pQsf, txn);
	}

	virtual ~FunctorMakeqsf() {}
};

/*!
 * @brief ST_MAKECONE(p0,p1,A)
 *
 * @attention Actually, called like ST_MAKECONE(p00,p01,p02,p10,p11,p12,A)
 * @return Quadratic Surface
 */
class FunctorMakecone : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 7) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		int i = 0;
		for (i = 0; i < 7; i++) {
			if (args[i]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[i]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument contains a non-numeric value");
			}
			if (!util::isFinite(D(args[i]))) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
					"Invalid value for coordinates");
			}
		}

		QuadraticSurface *pQsf = QP_NEW QuadraticSurface(txn, TR_PV3KEY_CONE, 7,
			D(args[0]), D(args[1]), D(args[2]), D(args[3]), D(args[4]),
			D(args[5]), D(args[6]));

		return Expr::newGeometryValue(pQsf, txn);
	}
};

/*!
 * @brief ST_MAKESPHERE(p0,R)
 *
 * @attention Actually, called like ST_MAKESPHERE(p00,p01,p02,R)
 * @return Quadratic Surface
 */
class FunctorMakesphere : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 4) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		int i = 0;
		for (i = 0; i < 4; i++) {
			if (args[i]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[i]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument contains a non-numeric value");
			}
			if (!util::isFinite(D(args[i]))) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
					"Invalid value for coordinates");
			}
		}

		QuadraticSurface *pQsf = QP_NEW QuadraticSurface(txn, TR_PV3KEY_SPHERE,
			4, D(args[0]), D(args[1]), D(args[2]), D(args[3]));

		return Expr::newGeometryValue(pQsf, txn);
	}

	virtual ~FunctorMakesphere() {}
};

/*!
 * @brief ST_MAKEPLANE(p0,p1)
 *
 * @attention Actually, called like ST_MAKEPLANE(p00,p01,p02,p10,p11,p12)
 * @return Quadratic Surface
 */
class FunctorMakeplane : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 6) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		int i = 0;
		for (i = 0; i < 6; i++) {
			if (args[i]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[i]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument contains a non-numeric value");
			}
			if (!util::isFinite(D(args[i]))) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
					"Invalid value for coordinates");
			}
		}

		QuadraticSurface *pQsf =
			QP_NEW QuadraticSurface(txn, TR_PV3KEY_PLANE, 6, D(args[0]),
				D(args[1]), D(args[2]), D(args[3]), D(args[4]), D(args[5]));

		return Expr::newGeometryValue(pQsf, txn);
	}

	virtual ~FunctorMakeplane() {}
};

/*!
 * @brief ST_MAKECYLINDER(p0,p1)
 *
 * @attention Actually, called like ST_MAKECYLINDER(p00,p01,p02,p10,p11,p12,R)
 * @return Quadratic Surface
 */
class FunctorMakecylinder : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 7) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		int i = 0;
		for (i = 0; i < 7; i++) {
			if (args[i]->isNullValue()) {
				return Expr::newNullValue(txn);
			}
			if (!args[i]->isNumeric()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					"Argument contains a non-numeric value");
			}
			if (!util::isFinite(D(args[i]))) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
					"Invalid value for coordinates");
			}
		}

		QuadraticSurface *pQsf = QP_NEW QuadraticSurface(txn,
			TR_PV3KEY_CYLINDER, 7, D(args[0]), D(args[1]), D(args[2]),
			D(args[3]), D(args[4]), D(args[5]), D(args[6]));

		return Expr::newGeometryValue(pQsf, txn);
	}

	virtual ~FunctorMakecylinder() {}
};

/*!
 * @brief MBR_INTERSECTS(g1, g2)
 *
 * @return g1's MBR intersects with g2's MBR.
 */
class FunctorMbrIntersects : public TqlGisFunc {
public:
	using TqlGisFunc::operator();
	Expr *operator()(
		QP_XArray<Expr *> &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue() || args[1]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		else if (!args[0]->isGeometry() || !args[1]->isGeometry()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument is not a geometry");
		}
		else {
			/* DO NOTHIHG */
		}
		Geometry *g1 = args[0]->getGeometry();
		Geometry *g2 = args[1]->getGeometry();
		bool x = g1->isBoundingRectIntersects(*g2);
		if (x) {
			return Expr::newBooleanValue(true, txn);
		}
		return Expr::newBooleanValue(false, txn);
	}

	/*!
	 * Check if the index is usable for fast search
	 * @return Applicable map type = MAP_TYPE_SPATIAL
	 */
	uint8_t checkUsableIndex() {
		return MAP_TYPE_SPATIAL;
	}

	/*!
	 * @brief Get the evaluated rectangle as a parameter to pass to index
	 *
	 * @param txn The transaction context
	 * @param args Arguments of ST_MbrIntersects (0,1: Geometry or Column)
	 * @param outSearchType Search type to use
	 * @param outParam1 will be a rectangle if successful
	 * @param outParam2 will be NULL.
	 */
	void getIndexParam(TransactionContext &txn, ExprList &args,
		GeometryOperator &outSearchType, const void *&outParam1,
		const void *&outParam2, ColumnId &indexColumnId) {
		if (args.size() != 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (!args[0]->isGeometry() && !args[1]->isGeometry() &&
				 !(args[0]->isColumn() && args[1]->isColumn())) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument is not a geometry, or geometry is not created");
		}
		else {
			/* DO NOTHIHG */
		}

		TrRect r1 = NULL;
		if (args[0]->isColumn() && args[1]->isGeometry()) {
			r1 = QP_NEW TrRectTag;
			*r1 = args[1]->getGeometry()->getBoundingRect();
			indexColumnId = args[0]->getColumnId();
		}
		else if (args[1]->isColumn() && args[0]->isGeometry()) {
			r1 = QP_NEW TrRectTag;
			*r1 = args[0]->getGeometry()->getBoundingRect();
			indexColumnId = args[1]->getColumnId();
		}
		else {
			indexColumnId = args[0]->getColumnId();
		}
		outSearchType = GEOMETRY_INTERSECT;
		outParam1 = reinterpret_cast<void *>(r1);
		outParam2 = NULL;
		return;
	}

	virtual ~FunctorMbrIntersects() {}
};

/*!
 * @brief QSF_MBR_INTERSECTS(g1, g2)
 *
 * @return g1 as QSF intersects with g2's MBR.
 */
class FunctorQsfmbrIntersects : public TqlGisFunc {
public:
	using TqlGisFunc::operator();
	Expr *operator()(
		QP_XArray<Expr *> &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue() || args[1]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		else if (!args[0]->isGeometry() || !args[1]->isGeometry()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument is not a geometry");
		}
		else {
			/* DO NOTHIHG */
		}

		Geometry *g1 = args[0]->getGeometry();
		Geometry *g2 = args[1]->getGeometry();

		if (g1->getType() != Geometry::QUADRATICSURFACE) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 2 must be a quadratic surface");
		}
		else if (g2->getDimension() != 3) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 must be a 3D box");
		}
		else {
			/* DO NOTHIHG */
		}

		TrRectTag r = g2->getBoundingRect();
		TrPv3Box box;
		box.p0[0] = r.xmin;
		box.p0[1] = r.ymin;
		box.p0[2] = r.zmin;
		box.p1[0] = r.xmax - r.xmin;
		box.p1[1] = r.ymax - r.ymin;
		box.p1[2] = r.zmax - r.zmin;


		QuadraticSurface *pQsf = static_cast<QuadraticSurface *>(g1);
		const TrPv3Key &key = pQsf->getPv3Key();
		int result = TrPv3Test2(&box, const_cast<TrPv3Key *>(&key));
		if (result != 0) {
			return Expr::newBooleanValue(true, txn);
		}
		else {
			return Expr::newBooleanValue(false, txn);
		}
	}

	/*!
	 * Check if the index is usable for fast search
	 * @return Applicable map type = MAP_TYPE_SPATIAL
	 */
	uint8_t checkUsableIndex() {
		return MAP_TYPE_SPATIAL;
	}

	/*!
	 * @brief Put evaluated QuadraticSurface into index parameter
	 *
	 * @param txn The transaction context
	 * @param args arguments (0: QSF, 1: Column or Geometry)
	 * @param outSearchType Search type to use
	 * @param outParam1 Will be put QSF if successful
	 * @param outParam2 will be NULL
	 */
	void getIndexParam(TransactionContext &txn, ExprList &args,
		GeometryOperator &outSearchType, const void *&outParam1,
		const void *&outParam2, ColumnId &indexColumnId) {
		if (args.size() != 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (!args[0]->isGeometry()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument is not a geometry, or geometry is not created");
		}
		else if (!args[1]->isColumn()) {
			outParam1 = NULL;
			outParam2 = NULL;
			indexColumnId = UNDEF_COLUMNID;
			return;
		}
		else {
			/* DO NOTHIHG */
			indexColumnId = args[1]->getColumnId();
		}

		Geometry *g1 = args[0]->getGeometry();
		if (g1->getType() != Geometry::QUADRATICSURFACE) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 2 must be a quadratic surface");
		}

		QuadraticSurface *pQsf = static_cast<QuadraticSurface *>(g1);
		const TrPv3Key &key = pQsf->getPv3Key();
		TrPv3Key *pkey = QP_NEW TrPv3Key;
		*pkey = key;

		outSearchType = GEOMETRY_QSF_INTERSECT;
		outParam1 = reinterpret_cast<void *>(pkey);
		outParam2 = NULL;
		return;
	}

	virtual ~FunctorQsfmbrIntersects() {}
};

/*!
 * @brief ST_GETSRID(g)
 * @return The SRID of the geometry
 */
class FunctorGetsrid : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isGeometry()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument is not a geometry");
		}

		Geometry *g1 = args[0]->getGeometry();
		return Expr::newNumericValue(g1->getSrId(), txn);
	}

	virtual ~FunctorGetsrid() {}
};

#endif
