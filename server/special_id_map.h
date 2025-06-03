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
	@brief Definition of TqlSpecialId functions for TQL
*/

#ifndef SPECIAL_ID_MAP_H_
#define SPECIAL_ID_MAP_H_

#include "function_map.h"

/*!
 * @brief Special ID used in a function (such as YEAR, MONTH, DAY, HOUR, MINUTE,
 * SECOND)
 *
 */
class TqlSpecialId : public TqlFunc {
public:
	TqlSpecialId(const char *const name) : name_(name) {}
	virtual ~TqlSpecialId() {}
	using TqlFunc::operator();
	virtual Expr *operator()(ExprList &, TransactionContext &, ObjectManagerV4 &, AllocateStrategy &);

	/*!
	 * @brief If function has internal valuables, returns a clone object of
	 * "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlSpecialId *clone(util::StackAllocator &) {
		return this;
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() { /* DO NOTHING */
	}

private:
	const char *const name_;
};

/*!
 * @brief Hash based map for Special IDs
 *
 */
class SpecialIdMap : public OpenHash<TqlSpecialId> {
public:
	static SpecialIdMap *getInstance() {
		return &map_;
	}
	/*!
	 * @brief Register functions
	 *
	 */
	SpecialIdMap() : OpenHash<TqlSpecialId>() {
		RegisterFunction("YEAR");
		RegisterFunction("MONTH");
		RegisterFunction("DAY");
		RegisterFunction("HOUR");
		RegisterFunction("MINUTE");
		RegisterFunction("SECOND");
		RegisterFunction("MILLISECOND");
		RegisterFunction("MICROSECOND");
		RegisterFunction("NANOSECOND");
		RegisterFunction("MAX");
		RegisterFunction("MIN");
		RegisterFunction("AVG");
	}

private:
	static SpecialIdMap map_;
	void RegisterFunction(const char *name) {
		TqlSpecialId *functor = new TqlSpecialId(name);
		insert(name, functor);
	}
};

#endif /* SPECIAL_ID_MAP_H_ */
