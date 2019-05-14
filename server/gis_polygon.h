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
	@brief Polygon class for 3D-geometry data
*/

#ifndef GIS_POLYGON_H_
#define GIS_POLYGON_H_

#include "util/container.h"
#include "gis_surface.h"
#include <sstream>

class MultiPolygon;
class PolyhedralSurface;

/*!
* @brief Polygon class
*
*/
class Polygon : public Surface {
	friend class MultiPolygon;
	friend class PolyhedralSurface;
	friend class Geometry;

public:
	/*!
	* @brief Constructor for empty object
	*/
	explicit Polygon(TransactionContext &txn) : Surface(txn) {
		isEmpty_ = true;
		isAssigned_ = true;
		srId_ = -1;
	}

	/*!
	* @brief Constructor
	*
	* @param id SRID
	* @param exteriorBorder LineString of exterior border (to be duplicated)
	* @param interiorBorders LineStrings of internal borders (to be duplicated)
	 * @param txn The transaction context
	 * @param txn Object manager
	*/
	Polygon(srid_t id, LinearRing *exteriorBorder,
		QP_XArray<LinearRing *> *interiorBorders, TransactionContext &txn,
		ObjectManager &objectManager)
		: Surface(id, exteriorBorder, interiorBorders, txn, objectManager) {
		/*
		  for(int i=0; i<interiorBorders->size();i++){
		  if(!exteriorBorder->crossGeometry(interiorBorders[i])){
		  GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_ERROR_OTHER,
		  "Invalid interior border");
		  }
		  for(int k=0; k<i; k++){
		  if(interiorBorders[i]->crossGeometry(interiorBorders[k])){
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_ERROR_OTHER,
			"Invalid interior border");
		  }
		  }
		  }
		*/



		isSimple_ = true;
		isEmpty_ = (exteriorBorder == NULL);
		srId_ = id;
		if (exteriorBorder == NULL) {
			dimension_ = 0;
		}
		else {
			dimension_ = exteriorBorder->dimension_;
			isAssigned_ = exteriorBorder->isAssigned();
			if (interiorBorders) {
				for (size_t i = 0; i < interiorBorders->size(); i++) {
					isAssigned_ &= (*interiorBorders)[i]->isAssigned();
				}
			}
		}
	}

	/*!
	* @brief Constructor from multipoint
	*
	* @param id SRID
	* @param mp multipoints that describe exterior border and interior borders
	* @param txn The transaction context
	* @param txn Object manager
	*/
	Polygon(srid_t id, QP_XArray<MultiPoint *> *mp, TransactionContext &txn,
		ObjectManager &objectManager)
		: Surface(txn) {
		QP_XArray<LinearRing *> *v = NULL;
		LinearRing *p;
		QP_XArray<MultiPoint *>::const_iterator it = mp->begin();
		if (it != mp->end()) {
			p = QP_NEW LinearRing(
				(*it)->getSrId(), (*it)->getPoints(), txn, objectManager);
		}
		else {
			p = NULL;
		}
		for (++it; it != mp->end(); ++it) {
			if (v == NULL) {
				v = QP_NEW QP_XArray<LinearRing *>(txn.getDefaultAllocator());
			}
			v->push_back(QP_NEW LinearRing(
				(*it)->getSrId(), (*it)->getPoints(), txn, objectManager));
		}
		setBorders(txn, objectManager, p, v);

		srId_ = id;
		isSimple_ = true;
		isEmpty_ = (p == NULL);
		isAssigned_ = true;

		QP_SAFE_DELETE(p);
		if (v != NULL) {
			while (!v->empty()) {
				QP_SAFE_DELETE(v->back());
				v->pop_back();
			}
			QP_SAFE_DELETE(v);
		}
	}

	/*!
	* @brief Destructor
	*/
	virtual ~Polygon() {
	}

	/*!
	* @brief Check simplicity
	*
	* @return result
	*/
	virtual bool isSimple() const {
		return true;
	}

	/*!
	* @brief Get type
	*
	* @return type
	*/
	virtual GeometryType getType() const {
		return POLYGON;
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
	* @brief Duplicate the object with given ID
	*
	* @param txn The transaction context
	* @param txn Object manager
	* @param id SRID
	*
	* @return duplicated object
	*/
	Polygon *dup(
		TransactionContext &txn, ObjectManager &objectManager, srid_t id) {
		if (isEmpty()) {
			return QP_NEW Polygon(txn);
		}
		else {
			Polygon *p = QP_NEW Polygon(
				id, exteriorBorder_, interiorBorders_, txn, objectManager);
			p->isAssigned_ = isAssigned_;
			p->isEmpty_ = isEmpty_;
			return p;
		}
	}
	/*!
	* @brief Duplicate the object
	* @param txn The transaction context
	* @param txn Object manager
	*/
	Polygon *dup(TransactionContext &txn, ObjectManager &objectManager) {
		return dup(txn, objectManager, srId_);
	}

	/*!
	* @brief Generate new assigned polygon object (not used)
	* ex) assign POLYGON((A 1 2, ...)) to A=0 -> new POLYGON((0 1 2, ...))
	*     is generated.
	* @param txn The transaction context
	* @param txn Object manager
	* @param amap Symbol map to assign used in expression evaluation
	* @param fmap Function map to call used in expression evaluation
	* @param mode EvalMode
	*
	* @return newly generated polygon object
	*/
	virtual Polygon *assign(TransactionContext &txn,
		ObjectManager &objectManager, ContainerRowWrapper *amap,
		FunctionMap *fmap, EvalMode mode = EVAL_MODE_NORMAL) {
		if (isAssigned_ || isEmpty_) {
			return static_cast<Polygon *>(dup(txn, objectManager));
		}

		LinearRing *exterior = static_cast<LinearRing *>(
			exteriorBorder_->assign(txn, objectManager, amap, fmap, mode));
		QP_XArray<LinearRing *> *interior;
		if (interiorBorders_ != NULL) {
			interior =
				QP_NEW QP_XArray<LinearRing *>(txn.getDefaultAllocator());
			for (size_t k = 0; k < interiorBorders_->size(); k++) {
				interior->push_back(
					static_cast<LinearRing *>((*interiorBorders_)[k]->assign(
						txn, objectManager, amap, fmap, mode)));
			}
		}
		else {
			interior = NULL;
		}
		Polygon *p =
			QP_NEW Polygon(srId_, exterior, interior, txn, objectManager);

		QP_SAFE_DELETE(exterior);
		if (interior != NULL) {
			while (!interior->empty()) {
				QP_DELETE(interior->back());
				interior->pop_back();
			}
			QP_SAFE_DELETE(interior);
		}

		return p;
	}

	/*!
	* @brief Cast to WKT String
	*
	* @return
	*/
	virtual const char *getString(TransactionContext &txn) const {
		util::NormalOStringStream os;
		os << "POLYGON(";
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
	* @brief Cast to WKT-sub string
	*
	* @param txn TransactionContext
	* @param os string-stream to save the result
	*/
	virtual void stringify(
		TransactionContext &txn, util::NormalOStringStream &os) const {
		if (isEmpty_) {
			os << "EMPTY";
		}
		else {
			os << '(';
			exteriorBorder_->stringify(txn, os);
			os << ')';
			if (interiorBorders_ != NULL) {
				os << ',';
				for (size_t k = 0; k < interiorBorders_->size(); k++) {
					os << '(';
					(*interiorBorders_)[k]->stringify(txn, os);
					if ((*interiorBorders_)[k]->getSrId() != -1) {
						os << ';' << (*interiorBorders_)[k]->getSrId();
					}
					os << ')';
				}
			}
		}
		if (getSrId() != -1) {
			os << ';' << getSrId();
		}
	}

	/*!
	* @brief Check for equality with a geometry
	*
	* @param g geometry to test
	*
	* @return result
	*/
	virtual bool operator==(Geometry &g) const {
		if (isEmpty() && g.isEmpty()) {
			return true;
		}
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Not implemented");
	}

protected:
	/*!
	 * @brief Get number of bytes of serialized array
	 * @return calculated size
	 */
	virtual size_t getSerializedSize() const {
		return this->Surface::getSerializedSize();
	}

	/*!
	 * @brief Serialize the geometry into output buffer
	 *
	 * @param out output buffer
	 * @param offset buffer's start offset
	 */
	void serializeObject(util::XArray<uint8_t> &out, uint64_t &offset) {
		this->Surface::serializeObject(out, offset);
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
	static Polygon *deserializeObject(TransactionContext &txn,
		const uint8_t *in, uint64_t &offset, int srId, int dimension) {
		Polygon *p = QP_NEW Polygon(txn);
		uint16_t nInterior;

		assignFromMemory(&nInterior, in + offset);
		offset += sizeof(uint16_t);

		p->dimension_ = dimension;
		p->exteriorBorder_ = static_cast<LinearRing *>(
			LineString::deserializeObject(txn, in, offset, srId, dimension));
		if (nInterior > 0) {
			p->interiorBorders_ =
				QP_NEW QP_XArray<LinearRing *>(txn.getDefaultAllocator());
			for (uint16_t i = 0; i < nInterior; i++) {
				QP_XArray<LinearRing *> &borders = *(p->interiorBorders_);
				borders.push_back(
					static_cast<LinearRing *>(LineString::deserializeObject(
						txn, in, offset, srId, dimension)));
			}
		}
		else {
			p->interiorBorders_ = NULL;
		}

		p->isSimple_ = true;
		p->isEmpty_ = (p->exteriorBorder_ == NULL);
		return p;
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
	static Polygon *deserializeObject(TransactionContext &txn,
		util::ByteStream<util::ArrayInStream> &in, int srId, int dimension) {
		Polygon *p = QP_NEW Polygon(txn);
		uint16_t nInterior;
		in >> nInterior;

		p->dimension_ = dimension;
		p->exteriorBorder_ = static_cast<LinearRing *>(
			LineString::deserializeObject(txn, in, srId, dimension));
		if (nInterior > 0) {
			p->interiorBorders_ =
				QP_NEW QP_XArray<LinearRing *>(txn.getDefaultAllocator());
			for (uint16_t i = 0; i < nInterior; i++) {
				QP_XArray<LinearRing *> &borders = *(p->interiorBorders_);
				borders.push_back(static_cast<LinearRing *>(
					LineString::deserializeObject(txn, in, srId, dimension)));
			}
		}
		else {
			p->interiorBorders_ = NULL;
		}

		p->isSimple_ = true;
		p->isEmpty_ = (p->exteriorBorder_ == NULL);
		return p;
	}
};

#endif /* GIS_POLYGON_HPP */
