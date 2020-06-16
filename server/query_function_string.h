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
	@brief Definition of query functions for string or LOB values
*/

#ifndef QUERY_FUNCTION_STRING_H_
#define QUERY_FUNCTION_STRING_H_

#include "query_function.h"


struct StringFunctions {

	struct Translate {
		struct Policy {
			typedef util::TrueType Allocatable;
		};

		template<typename C, typename R>
		typename C::WriterType& operator()(
				C &cxt, R &subject, R &search, R &replace);
	};
};


template<typename C, typename R>
inline typename C::WriterType& StringFunctions::Translate::operator()(
		C &cxt, R &subject, R &search, R &replace) {
	typedef typename R::CodeType SearchCodePoint;
	typedef typename R::CodeType ReplaceCodePoint;
	typedef std::map<SearchCodePoint, ReplaceCodePoint> BaseReplaceMap;
	typedef typename BaseReplaceMap::key_compare ReplaceCompare;
	typedef typename BaseReplaceMap::value_type ReplaceValue;
	typedef typename C::AllocatorType::template rebind<ReplaceValue>::other ReplaceAlloc;
	typedef std::map<SearchCodePoint, ReplaceCodePoint, ReplaceCompare, ReplaceAlloc> ReplaceMap;
	ReplaceMap replaceMap(ReplaceCompare(), cxt.getAllocator());

	SearchCodePoint searchCodePoint;
	ReplaceCodePoint replaceCodePoint;
	const util::CodePoint erasedCodePoint =
			std::numeric_limits<util::CodePoint>::max();

	while (search.nextCode(&searchCodePoint)) {
		if (replaceMap.find(searchCodePoint) == replaceMap.end()) {
			if (replace.nextCode(&replaceCodePoint)) {
				replaceMap[searchCodePoint] = replaceCodePoint;
			}
			else {
				replaceMap[searchCodePoint] = erasedCodePoint;
			}
		}
		else {
			replace.nextCode(&replaceCodePoint);
		}
	}

	typename R::CodeType subjectCodePoint;
	typename C::WriterType &ret = cxt.getResultWriter();
	while (subject.nextCode(&subjectCodePoint)) {
		typename ReplaceMap::iterator replaceItr =
				replaceMap.find(subjectCodePoint);
		if (replaceItr != replaceMap.end()) {
			if (replaceItr->second != erasedCodePoint) {
				ret.appendCode(replaceItr->second);
			}
		}
		else {
			ret.appendCode(subjectCodePoint);
		}
	}

	return ret;
}

#endif
