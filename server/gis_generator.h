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
	@brief Generator functors for GIS objects
*/

#ifndef GIS_GENERATOR_H_
#define GIS_GENERATOR_H_

#include "function_map.h"
#include "gis_geometry.h"
#include "gis_linearring.h"
#include "gis_linestring.h"
#include "gis_multipoint.h"
#include "gis_multipolygon.h"
#include "gis_point.h"
#include "gis_pointgeom.h"
#include "gis_polygon.h"
#include "gis_polyhedralsurface.h"
#include "gis_quadraticsurface.h"
#include "gis_surface.h"
#include "qp_def.h"

/*!
 * @brief Functor to generate a point from TQL
 */
class PointGenerator : public TqlFunc {
public:
	using TqlFunc::operator();
	/*!
	 * Operator() : Functor main, generates a Point expression
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return Generated expression
	 */
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot evaluate function with empty argument.");
		}
		else if (args[0]->isNullValue() || (args.size() == 2 && args[1]->isNullValue())) {
			return Expr::newNullValue(txn);
		}
		else if (args.size() == 1 &&
				 (util::stricmp(args[0]->getValueAsString(txn), "EMPTY") ==
					 0)) {
			return Expr::newGeometryValue(QP_NEW_BY_TXN(txn) Point(txn), txn);
		}

		if (args.size() == 2) {
			Expr *e = args[0];
			Expr *e2 = args[1];
			if (e->isGeometry() && e2->isNumeric()) {
				Geometry *geom = e->getGeometry();
				if (geom == NULL) {
					GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_ERROR_OTHER,
						"Cannot get source geometry");
				}
				Geometry::GeometryType t = geom->getType();
				if (t == Geometry::MULTIPOINT) {
					MultiPoint *mp = static_cast<MultiPoint *>(geom);
					if (mp->numPoints() == 1) {
						Point &p = (mp->getPoint(0));
						return Expr::newGeometryValue(
							p.dup(txn, objectManager, strategy,
								static_cast<Geometry::srid_t>(int(*e2))),
							txn);
					}
				}
			}
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"WKT argument is invalid");
	}
};

/*!
 * @brief Functor to generate MultiPoint from TQL
 *
 */
class MultiPointGenerator : public TqlFunc {
public:
	using TqlFunc::operator();
	/*!
	 * Operator() : Functor main, generates a MultiPoint expression
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return Generated expression
	 */
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot evaluate function with empty argument.");
		}
		else if (args[0]->isNullValue() || (args.size() == 2 && args[1]->isNullValue())) {
			return Expr::newNullValue(txn);
		}
		else if (args.size() == 1 &&
				 (util::stricmp(args[0]->getValueAsString(txn), "EMPTY") ==
					 0)) {
			return Expr::newGeometryValue(
				QP_NEW_BY_TXN(txn) MultiPoint(txn), txn);
		}

		if (args.size() == 2) {
			Expr *e = args[0];
			Expr *e2 = args[1];
			if (e->isGeometry() && e2->isNumeric()) {
				Geometry *geom = e->getGeometry();
				Geometry::GeometryType t = geom->getType();
				if (t == Geometry::MULTIPOINT) {
					MultiPoint *m = static_cast<MultiPoint *>(geom)->dup(txn,
						objectManager, strategy, static_cast<Geometry::srid_t>(int(*e2)));
					return Expr::newGeometryValue(m, txn);
				}
			}
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"WKT argument is invalid");
	}
};

/*!
 * @brief Functor to generate LineString from TQL
 *
 */
class LineStringGenerator : public TqlFunc {
public:
	using TqlFunc::operator();
	/*!
	 * Operator() : Functor main, generates a LineString expression
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return Generated expression
	 */
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot evaluate function with empty argument.");
		}
		else if (args[0]->isNullValue() || (args.size() == 2 && args[1]->isNullValue())) {
			return Expr::newNullValue(txn);
		}
		else if (args.size() == 1 &&
				 (util::stricmp(args[0]->getValueAsString(txn), "EMPTY") ==
					 0)) {
			return Expr::newGeometryValue(
				QP_NEW_BY_TXN(txn) LineString(txn), txn);
		}
		if (args.size() == 2) {
			Expr *e1 = args[0], *e2 = args[1];
			if (e1->isGeometry() && e2->isNumeric()) {
				Geometry *geom = e1->getGeometry();
				Geometry::srid_t id = static_cast<Geometry::srid_t>(int(*e2));
				if (geom->getType() == Geometry::MULTIPOINT) {
					QP_XArray<Point *> parray(txn.getDefaultAllocator());
					MultiPoint *mp =
						static_cast<MultiPoint *>(e1->getGeometry());
					if (mp->numPoints() >= 2) {
						for (int i = 0; i < mp->numPoints(); i++) {
							parray.push_back(
								dynamic_cast<Point *>(&mp->getPoint(i)));
						}
						return Expr::newGeometryValue(
							QP_NEW_BY_TXN(txn)
								LineString(id, parray, txn, objectManager, strategy),
							txn);
					}
				}
			}
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"WKT argument is invalid");
	}
};

/*!
 * @brief LinearRing Generator: Functor to generate linearring from TQL
 *
 */
class LinearRingGenerator : public TqlFunc {
public:
	using TqlFunc::operator();
	/*!
	 * Operator() : Functor main, generates a LinearRing expression
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return Generated expression
	 */
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot evaluate function with empty argument.");
		}
		else if (args[0]->isNullValue() || (args.size() == 2 && args[1]->isNullValue())) {
			return Expr::newNullValue(txn);
		}
		else if (args.size() == 1 &&
				 (util::stricmp(args[0]->getValueAsString(txn), "EMPTY") ==
					 0)) {
			return Expr::newGeometryValue(
				QP_NEW_BY_TXN(txn) LinearRing(txn), txn);
		}

		if (args.size() == 2) {
			Expr *e1 = args[0], *e2 = args[1];
			if (e1->isGeometry() && e2->isNumeric()) {
				Geometry *geom = e1->getGeometry();
				Geometry::srid_t id = static_cast<Geometry::srid_t>(int(*e2));
				if (geom->getType() == Geometry::MULTIPOINT) {
					QP_XArray<Point *> parray(txn.getDefaultAllocator());
					MultiPoint *mp =
						static_cast<MultiPoint *>(e1->getGeometry());
					if (mp->numPoints() >= 2) {
						for (int i = 0; i < mp->numPoints(); i++) {
							parray.push_back(
								dynamic_cast<Point *>(&mp->getPoint(i)));
						}
						return Expr::newGeometryValue(
							QP_NEW_BY_TXN(txn)
								LinearRing(id, parray, txn, objectManager, strategy),
							txn);
					}
				}
			}
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"WKT argument is invalid");
	}
};

/*!
 * @brief Functor to generate polygon from TQL
 *
 */
class PolygonGenerator : public TqlFunc {
public:
	using TqlFunc::operator();
	/*!
	 * Operator() : Functor main, generates a Polygon expression
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return Generated expression
	 */
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot evaluate function with empty argument.");
		}
		else if (args[0]->isNullValue() || (args.size() == 2 && args[1]->isNullValue())) {
			return Expr::newNullValue(txn);
		}
		else if (args.size() == 1 &&
				 (util::stricmp(args[0]->getValueAsString(txn), "EMPTY") ==
					 0)) {
			return Expr::newGeometryValue(QP_NEW_BY_TXN(txn) Polygon(txn), txn);
		}

		Expr *e1 = args[0];
		Expr *e2 = args[1];
		if (e1->isGeometry() && e2->isNumeric()) {
			Geometry *geom = e1->getGeometry();
			if (args.size() == 2 && geom->getType() == Geometry::POLYGON) {
				Geometry::srid_t id = static_cast<Geometry::srid_t>(int(*e2));
				Polygon *p = static_cast<Polygon *>(geom);
				return Expr::newGeometryValue(
					p->dup(txn, objectManager, strategy, id), txn);
			}
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"WKT argument is invalid");
	}
};

/*!
 * @brief Functor to generate PolyhedronGenerator from TQL
 *
 */
class PolyhedronGenerator : public TqlFunc {
public:
	using TqlFunc::operator();
	/*!
	 * Operator() : Functor main, generates a PolyhedralSurface expression
	 *
	 * @param txn The transaction context
	 * @param txn Object manager
	 * @return Generated expression
	 */
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot evaluate function with empty argument.");
		}
		else if (args[0]->isNullValue() || (args.size() == 2 && args[1]->isNullValue())) {
			return Expr::newNullValue(txn);
		}
		else if (args.size() == 1 &&
				 (util::stricmp(args[0]->getValueAsString(txn), "EMPTY") ==
					 0)) {
			return Expr::newGeometryValue(
				QP_NEW_BY_TXN(txn) PolyhedralSurface(txn), txn);
		}

		Expr *e1 = args[0];
		Expr *e2 = args[1];
		if (e1->isGeometry() && e2->isNumeric()) {
			Geometry *geom = e1->getGeometry();
			if (args.size() == 2 && geom->getType() == Geometry::MULTIPOLYGON) {
				Geometry::srid_t id = static_cast<Geometry::srid_t>(int(*e2));
				return Expr::newGeometryValue(
					QP_NEW_BY_TXN(txn) PolyhedralSurface(id,
						*static_cast<MultiPolygon *>(geom), txn, objectManager, strategy),
					txn);
			}
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"WKT argument is invalid");
	}
};

/*!
 * @brief Functor to generate PolyhedronGenerator from TQL
 *
 */
class QuadraticSurfaceGenerator : public TqlFunc {
public:
	using TqlFunc::operator();
	/*!
	 * Operator() : Functor main, generates a QuadraticSurface expression
	 *
	 * @return Generated expression
	 */
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
		if (args.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot evaluate function with empty argument.");
		}
		else if (args[0]->isNullValue() || (args.size() == 2 && args[1]->isNullValue())) {
			return Expr::newNullValue(txn);
		}
		else if (args.size() == 1 &&
				 (util::stricmp(args[0]->getValueAsString(txn), "EMPTY") ==
					 0)) {
			return Expr::newGeometryValue(
				QP_NEW_BY_TXN(txn) QuadraticSurface(txn), txn);
		}

		Expr *e1 = args[0];
		Expr *e2 = args[1];
		if (e1->isGeometry() && e2->isNumeric()) {
			Geometry *geom = e1->getGeometry();
			if (args.size() == 2 &&
				geom->getType() == Geometry::QUADRATICSURFACE) {
				Geometry::srid_t id = static_cast<Geometry::srid_t>(int(*e2));
				return Expr::newGeometryValue(
					geom->dup(txn, objectManager, strategy, id), txn);
			}
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"WKT argument is invalid");
	}
};

#endif /* GIS_GENERATOR_HPP */
