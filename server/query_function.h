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
	@brief Definition of common feature of query functions
*/

#ifndef QUERY_FUNCTION_H_
#define QUERY_FUNCTION_H_


#include "util/type.h"
#include "util/numeric.h"
#include "gs_error.h"

struct MicroTimestamp;
struct NanoTimestamp;


struct FunctionUtils {


	struct BasicValueUtils;

	template<typename Alloc = util::FalseType, typename W = util::FalseType>
	class BasicContext;

	struct NumberArithmetic;

	struct TimeFieldChecker;
	struct TimeErrorHandler;

	template<typename T>
	struct DateTimeOf;

	struct AggregationValueOperations;
	template<
			typename A0,
			typename A1 = util::FalseType,
			typename A2 = util::FalseType>
	struct AggregationValues;


	static BasicContext<> createContext();

	template<typename Alloc, typename W>
	static BasicContext<Alloc, W> createContext(const Alloc &alloc, W &writer);


	template<typename C>
	static util::TimeZone resolveTimeZone(
			C &funcCxt, const util::TimeZone &zone);

	template<typename R>
	static util::TimeZone resolveTimeZone(R &reader);

	static util::PreciseDateTime toPreciseDateTime(const util::DateTime &src);
	static util::PreciseDateTime toPreciseDateTime(
			const util::PreciseDateTime &src);

	static void checkTimeField(int64_t fieldType, bool precise, bool days);
};

struct FunctionUtils::BasicValueUtils {
	bool isSpaceChar(util::CodePoint c) const;
};

template<typename Alloc, typename W>
class FunctionUtils::BasicContext {
public:
	typedef Alloc AllocatorType;
	typedef W WriterType;
	typedef BasicValueUtils ValueUtilsType;

	explicit BasicContext(
			const AllocatorType &alloc = AllocatorType(),
			WriterType *writer = NULL);

	BasicContext(const BasicContext &another);
	BasicContext& operator=(const BasicContext &another);

	BasicContext& operator*() const;

	AllocatorType getAllocator() const;
	WriterType& getResultWriter() const;
	const ValueUtilsType& getValueUtils() const;

	util::TimeZone getTimeZone() const;
	void setTimeZone(const util::TimeZone &zone);

	void applyDateTimeOption(util::DateTime::ZonedOption &option) const;
	void applyDateTimeOption(util::DateTime::Option &option) const;
	void setMaxTime(int64_t millis, const util::DateTime::FieldData *fields);

private:
	BasicContext *ref_;

	AllocatorType alloc_;
	WriterType *writer_;
	ValueUtilsType valueUtils_;

	util::TimeZone zone_;

	int64_t maxTimeMillis_;
	const util::DateTime::FieldData *maxTimeFields_;
};

struct FunctionUtils::NumberArithmetic {
public:
	template<typename T> static T add(const T &value1, const T &value2);
	template<typename T> static T subtract(const T &value1, const T &value2);
	template<typename T> static T multiply(const T &value1, const T &value2);
	template<typename T> static T divide(const T &value1, const T &value2);
	template<typename T> static T remainder(const T &value1, const T &value2);

	static bool middleOfOrderedValues(int64_t value1, int64_t value2, int64_t &result);
	static bool middleOfOrderedValues(double value1, double value2, double &result);

	static int64_t interpolateLinear(
			int64_t x1, int64_t y1, int64_t x2, int64_t y2, int64_t x);
	static int64_t interpolateLinear(
			double x1, int64_t y1, double x2, int64_t y2, double x);
	static double interpolateLinear(
			double x1, double y1, double x2, double y2, double x);

	static int32_t compareDoubleLongPrecise(double value1, int64_t value2);

private:
	struct ErrorHandler;

	typedef util::NumberArithmetic Base;
	typedef util::ArithmeticErrorHandlers::Strict BaseErrorHandler;
};

struct FunctionUtils::NumberArithmetic::ErrorHandler {
	void errorZeroDivision() const;
	void errorValueOverflow() const;
};

struct FunctionUtils::TimeFieldChecker {
public:
	TimeFieldChecker();

	TimeFieldChecker withPrecise(bool on = true) const;
	TimeFieldChecker withDays(bool on = true) const;

	void operator()(int64_t fieldType) const;

private:
	bool precise_;
	bool days_;
};

struct FunctionUtils::TimeErrorHandler {
	void errorTimeParse(std::exception &e) const;
	void errorTimeFormat(std::exception &e) const;
};

template<typename T>
struct FunctionUtils::DateTimeOf {
	typedef
			typename util::Conditional<
					util::IsSame<T, int64_t>::VALUE, util::DateTime,
			typename util::Conditional<
					util::IsSame<T, MicroTimestamp>::VALUE, util::PreciseDateTime,
			typename util::Conditional<
					util::IsSame<T, NanoTimestamp>::VALUE, util::PreciseDateTime,
			void>::Type>::Type>::Type Type;
};

struct FunctionUtils::AggregationValueOperations {
	template<typename T> struct ValueTraits {
		typedef T BaseType;
		static const T& toFullType(const T &src, bool) { return src; }
	};
	template<typename T> struct ValueTraits< std::pair<T, bool> > {
		typedef T BaseType;
		static std::pair<T, bool> toFullType(const T &src, bool assigned) {
			return std::pair<T, bool>(src, assigned);
		}
	};

	template<uint32_t N, typename T> struct Getter {
		typedef typename ValueTraits<T>::BaseType BaseType;
		template<typename C> BaseType operator()(C &cxt) const {
			return cxt.template getAggrValue<N, BaseType>();
		}
		template<typename C> T operator()(
				C &cxt, const util::TrueType&) const {
			if (cxt.template checkNullAggrValue<N, T>()) {
				return ValueTraits<T>::toFullType(BaseType(), false);
			}
			return ValueTraits<T>::toFullType(
					cxt.template getAggrValue<N, BaseType>(), true);
		}
	};
	template<uint32_t N, typename T, typename Promo> struct Setter {
		template<typename C, typename U>
		void operator()(C &cxt, const U &value) const {
			typedef typename ValueTraits<T>::BaseType BaseType;
			return cxt.template setAggrValue<N, BaseType>(value, Promo());
		}
		template<typename C, typename U>
		void operator()(C &cxt, U &value) const {
			typedef typename ValueTraits<U>::BaseType BaseType;
			return cxt.template setAggrValue<N, BaseType>(value, Promo());
		}
	};
	template<uint32_t N, typename T, typename Checked> struct Adder {
		template<typename C, typename U>
		void operator()(C &cxt, const U &value) const {
			typedef typename ValueTraits<T>::BaseType BaseType;
			return cxt.template addAggrValue<N, BaseType, Checked>(value);
		}
		template<typename C, typename U>
		void operator()(C &cxt, U &value) const {
			typedef typename ValueTraits<U>::BaseType BaseType;
			return cxt.template addAggrValue<N, BaseType, Checked>(value);
		}
	};
	template<uint32_t N, typename T> struct Incrementer {
		template<typename C> void operator()(C &cxt) {
			typedef typename ValueTraits<T>::BaseType BaseType;
			return cxt.template incrementAggrValue<N, BaseType>();
		}
	};
	template<uint32_t N, typename T> struct NullChecker {
		template<typename C> bool operator()(C &cxt) {
			typedef typename ValueTraits<T>::BaseType BaseType;
			return cxt.template checkNullAggrValue<N, BaseType>();
		}
	};
};

template<typename A0, typename A1, typename A2>
struct FunctionUtils::AggregationValues {
	template<uint32_t N>
	struct At {
		typedef
				typename util::Conditional<N == 0, A0,
				typename util::Conditional<N == 1, A1,
				typename util::Conditional<N == 2, A2, util::FalseType
				>::Type>::Type>::Type Type;
		typedef AggregationValueOperations Ops;

		typedef typename Ops::Getter<N, Type> Getter;
		typedef typename Ops::Setter<N, Type, util::FalseType> Setter;
		typedef typename Ops::Setter<N, Type, util::TrueType> PromotedSetter;
		typedef typename Ops::Adder<N, Type, util::TrueType> Adder;
		typedef typename Ops::Adder<N, Type, util::FalseType> UncheckedAdder;
		typedef typename Ops::Incrementer<N, Type> Incrementer;
		typedef typename Ops::NullChecker<N, Type> NullChecker;
	};

	template<uint32_t N> typename At<N>::Getter get() const {
		return typename At<N>::Getter();
	}
	template<uint32_t N> typename At<N>::Setter set() const {
		return typename At<N>::Setter();
	}
	template<uint32_t N> typename At<N>::PromotedSetter setPromoted() const {
		return typename At<N>::PromotedSetter();
	}
	template<uint32_t N> typename At<N>::Adder add() const {
		return typename At<N>::Adder();
	}
	template<uint32_t N> typename At<N>::UncheckedAdder addUnchecked() const {
		return typename At<N>::UncheckedAdder();
	}
	template<uint32_t N> typename At<N>::Incrementer increment() const {
		return typename At<N>::Incrementer();
	}
	template<uint32_t N> typename At<N>::NullChecker isNull() const {
		return typename At<N>::NullChecker();
	}
};



inline FunctionUtils::BasicContext<> FunctionUtils::createContext() {
	return BasicContext<>();
}

template<typename Alloc, typename W>
FunctionUtils::BasicContext<Alloc, W> FunctionUtils::createContext(
		const Alloc &alloc, W &writer) {
	return BasicContext<Alloc, W>(alloc, &writer);
}

template<typename C>
util::TimeZone FunctionUtils::resolveTimeZone(
		C &funcCxt, const util::TimeZone &zone) {
	if (!zone.isEmpty()) {
		return zone;
	}
	return funcCxt.getTimeZone();
}

template<typename R>
util::TimeZone FunctionUtils::resolveTimeZone(R &reader) {
	return util::TimeZone::readFrom(reader, TimeErrorHandler());
}

inline util::PreciseDateTime FunctionUtils::toPreciseDateTime(
		const util::DateTime &src) {
	return util::PreciseDateTime::ofNanoSeconds(src, 0);
}

inline util::PreciseDateTime FunctionUtils::toPreciseDateTime(
		const util::PreciseDateTime &src) {
	return src;
}


template<typename Alloc, typename W>
FunctionUtils::BasicContext<Alloc, W>::BasicContext(
		const AllocatorType &alloc, WriterType *writer) :
		ref_(NULL),
		alloc_(alloc),
		writer_(writer),
		maxTimeMillis_(util::DateTime::Option().maxTimeMillis_),
		maxTimeFields_(util::DateTime::ZonedOption().maxFields_) {
	ref_ = this;
}

template<typename Alloc, typename W>
FunctionUtils::BasicContext<Alloc, W>::BasicContext(
		const BasicContext &another) :
		ref_(NULL),
		writer_(another.writer_) {
	ref_ = this;
}

template<typename Alloc, typename W>
FunctionUtils::BasicContext<Alloc, W>&
FunctionUtils::BasicContext<Alloc, W>::operator=(const BasicContext &another) {
	writer_ = another.writer_;
	return *this;
}

template<typename Alloc, typename W>
FunctionUtils::BasicContext<Alloc, W>&
FunctionUtils::BasicContext<Alloc, W>::operator*() const {
	return *ref_;
}

template<typename Alloc, typename W>
typename FunctionUtils::BasicContext<Alloc, W>::AllocatorType
FunctionUtils::BasicContext<Alloc, W>::getAllocator() const {
	return alloc_;
}

template<typename Alloc, typename W>
typename FunctionUtils::BasicContext<Alloc, W>::WriterType&
FunctionUtils::BasicContext<Alloc, W>::getResultWriter() const {
	assert(writer_ != NULL);
	return *writer_;
}

template<typename Alloc, typename W>
const typename FunctionUtils::BasicContext<Alloc, W>::ValueUtilsType&
FunctionUtils::BasicContext<Alloc, W>::getValueUtils() const {
	return valueUtils_;
}

template<typename Alloc, typename W>
util::TimeZone FunctionUtils::BasicContext<Alloc, W>::getTimeZone() const {
	return zone_;
}

template<typename Alloc, typename W>
void FunctionUtils::BasicContext<Alloc, W>::setTimeZone(
		const util::TimeZone &zone) {
	zone_ = zone;
}

template<typename Alloc, typename W>
void FunctionUtils::BasicContext<Alloc, W>::applyDateTimeOption(
		util::DateTime::ZonedOption &option) const {
	option.maxFields_ = maxTimeFields_;
}

template<typename Alloc, typename W>
void FunctionUtils::BasicContext<Alloc, W>::applyDateTimeOption(
		util::DateTime::Option &option) const {
	option.maxTimeMillis_ = maxTimeMillis_;
}

template<typename Alloc, typename W>
void FunctionUtils::BasicContext<Alloc, W>::setMaxTime(
		int64_t millis, const util::DateTime::FieldData *fields) {
	maxTimeMillis_ = millis;
	maxTimeFields_ = fields;
}


template<typename T> inline T FunctionUtils::NumberArithmetic::add(
		const T &value1, const T &value2) {
	return Base::add(value1, value2, ErrorHandler());
}

template<typename T> inline T FunctionUtils::NumberArithmetic::subtract(
		const T &value1, const T &value2) {
	return Base::subtract(value1, value2, ErrorHandler());
}

template<typename T> inline T FunctionUtils::NumberArithmetic::multiply(
		const T &value1, const T &value2) {
	return Base::multiply(value1, value2, ErrorHandler());
}

template<typename T> inline T FunctionUtils::NumberArithmetic::divide(
		const T &value1, const T &value2) {
	return Base::divide(value1, value2, ErrorHandler());
}

template<typename T> inline T FunctionUtils::NumberArithmetic::remainder(
		const T &value1, const T &value2) {
	return Base::remainder(value1, value2, ErrorHandler());
}


inline FunctionUtils::TimeFieldChecker::TimeFieldChecker() :
		precise_(false),
		days_(false) {
}

inline FunctionUtils::TimeFieldChecker
FunctionUtils::TimeFieldChecker::withPrecise(bool on) const {
	TimeFieldChecker ret = *this;
	ret.precise_ = on;
	return ret;
}

inline FunctionUtils::TimeFieldChecker
FunctionUtils::TimeFieldChecker::withDays(bool on) const {
	TimeFieldChecker ret = *this;
	ret.days_ = on;
	return ret;
}

inline void FunctionUtils::TimeFieldChecker::operator()(
		int64_t fieldType) const {
	return checkTimeField(fieldType, precise_, days_);
}

#endif
