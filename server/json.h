/*
	Copyright (c) 2015 TOSHIBA CORPORATION.

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
	@brief Definition of JsonUtils
*/
#ifndef JSON_H_
#define JSON_H_

#include "util/code.h"
#include <limits>
#include <vector>
#include <map>

namespace picojson {
class value;
struct null;
}

struct JsonUtils {
public:
	typedef picojson::value Value;
	typedef picojson::null Null;
	typedef std::vector<picojson::value> Array;
	typedef std::map<std::string, picojson::value> Object;

	struct Path;


	static picojson::value parseAll(const char8_t *begin, const char8_t *end);
	static u8string parseAll(
			picojson::value &value, const char8_t *begin, const char8_t *end);

	template<typename T>
	static const T& as(const picojson::value &src, const Path *path = NULL);
	template<typename T>
	static T& as(picojson::value &src, const Path *path = NULL);

	template<typename T>
	static const T& as(
			const picojson::value &src, const u8string &key,
			Path *path = NULL);
	template<typename T>
	static T& as(
			picojson::value &src, const u8string &key, Path *path = NULL);

	template<typename T>
	static T asInt(const picojson::value &src, const Path *path = NULL);
	template<typename T>
	static T asInt(
			const picojson::value &src, const u8string &key,
			Path *path = NULL);

	template<typename T>
	static T asInt(const double &src, const Path *path = NULL);

	template<typename T>
	static void checkType(const picojson::value &src, const Path *path = NULL);

	template<typename T>
	static const T* find(const picojson::value &src);
	template<typename T>
	static T* find(picojson::value &src);

	template<typename T>
	static const T* find(
			const picojson::value &src,
			const u8string &key, Path *path = NULL);
	template<typename T>
	static T* find(
			picojson::value &src, const u8string &key, Path *path = NULL);

	template<typename T>
	static bool findInt(const picojson::value &src, T *result, Path *path = NULL);
	template<typename T>
	static bool findInt(
			const picojson::value &src, const u8string &key, T *result,
			Path *path = NULL);

	template<typename T>
	static const char8_t* typeToString();
	static const char8_t* typeToString(const picojson::value &value);

private:
	struct Impl;

	static const size_t MAX_ERROR_LINE_LENGTH;

	template<typename T, typename U> struct ConstTraits;

	template<typename T, bool = std::numeric_limits<T>::is_signed>
	struct LargestIntTraits;

	template<typename T>
	static const T& as(const T &src, const Path*);
	template<typename T>
	static T& as(T &src, const Path*);

	template<typename T>
	static const T* find(const T &src);
	template<typename T>
	static T* find(T &src);

	template<typename T>
	static bool is(const picojson::value &value);
	template<typename V, typename T>
	static bool is(const V &value, const util::TrueType&);
	template<typename V, typename T>
	static bool is(const V &value, const util::FalseType&);

	template<typename T>
	static const T& get(const picojson::value &value);
	template<typename T>
	static T& get(picojson::value &value);
	template<typename V, typename T, typename U>
	static T& get(V &value, const util::TrueType&);
	template<typename V, typename T, typename U>
	static T& get(V &value, const util::FalseType&);

	template<typename V, typename T, typename U>
	static T& valueAs(V &src, const u8string &key, Path *path);
	template<typename V, typename T, typename U>
	static T* findFromValue(V &src, const u8string &key, Path *path);
	template<typename T>
	static T* applyPath(T *value, const Path &localPath, Path *path);

	template<typename V, typename S>
	static V& asValue(S &src, const u8string &key, Path *path);
	template<typename V, typename S>
	static V* findValue(S &src, const u8string &key, Path *path);


	template<typename T>
	static void errorByIntRange(double value, T min, T max, const Path *path);

	static void errorByNoInt(double value, const Path *path);
	static void errorByType(
			const char8_t *expected, const char8_t *actual, const Path *path);
};

template<>
void JsonUtils::checkType<picojson::value>(
		const picojson::value &src, const Path *path);

struct JsonUtils::Path {
public:
	Path();

	Path child() const;
	Path named(const char8_t *name) const;
	Path indexed(size_t index) const;

	const Path* getParent() const;
	const char8_t* getName() const;
	size_t getIndex() const;

	bool isEmpty(bool recursive) const;
	bool isIndexed(bool recursive) const;

private:
	const Path *parent_;
	const char8_t *name_;
	size_t index_;
	bool indexed_;
};

std::ostream& operator<<(std::ostream &stream, const JsonUtils::Path *path);
std::ostream& operator<<(std::ostream &stream, const JsonUtils::Path &path);


template<typename T, typename U>
struct JsonUtils::ConstTraits {
	typedef U Type;
	typedef typename U::iterator Itr;
};

template<typename T, typename U>
struct JsonUtils::ConstTraits<const T, U> {
	typedef const U Type;
	typedef typename U::const_iterator Itr;
};

template<typename T, bool>
struct JsonUtils::LargestIntTraits {
	typedef int64_t Type;
};

template<typename T>
struct JsonUtils::LargestIntTraits<T, false> {
	typedef uint64_t Type;
};

template<typename T>
const T& JsonUtils::as(const picojson::value &src, const Path *path) {
	checkType<T>(src, path);
	return get<T>(src);
}

template<typename T>
T& JsonUtils::as(picojson::value &src, const Path *path) {
	checkType<T>(src, path);
	return get<T>(src);
}

template<typename T>
const T& JsonUtils::as(
		const picojson::value &src, const u8string &key, Path *path) {
	return valueAs<const picojson::value, const T, T>(src, key, path);
}

template<typename T>
T& JsonUtils::as(
		picojson::value &src, const u8string &key, Path *path) {
	return valueAs<picojson::value, T, T>(src, key, path);
}

template<typename T>
T JsonUtils::asInt(const picojson::value &src, const Path *path) {
	return asInt<T>(as<double>(src, path), path);
}

template<typename T>
T JsonUtils::asInt(
		const picojson::value &src, const u8string &key, Path *path) {
	Path localPath = (path == NULL ? Path() : *path);
	Path *localPathAddr = (path == NULL ? NULL : &localPath);

	const T &result =
			asInt<T>(as<double>(src, key, localPathAddr), localPathAddr);
	return *applyPath(&result, localPath, path);
}

template<typename T>
T JsonUtils::asInt(const double &src, const Path *path) {
	UTIL_STATIC_ASSERT(std::numeric_limits<T>::is_integer);
	typedef typename LargestIntTraits<T>::Type Largest;

	const Largest min = std::numeric_limits<T>::min();
	const Largest max = std::numeric_limits<T>::max();
	const volatile Largest intValue = static_cast<Largest>(src);

	if (!(static_cast<double>(min) <= src &&
			src <= static_cast<double>(max) &&
			min <= intValue && intValue <= max)) {
		errorByIntRange(src, min, max, path);
	}

	if (!(static_cast<double>(intValue) == src)) {
		errorByNoInt(src, path);
	}

	return static_cast<T>(intValue);
}

template<typename T>
void JsonUtils::checkType(const picojson::value &src, const Path *path) {
	UTIL_STATIC_ASSERT(
			(util::IsSame<T, picojson::null>::VALUE) ||
			(util::IsSame<T, bool>::VALUE) ||
			(util::IsSame<T, double>::VALUE) ||
			(util::IsSame<T, std::string>::VALUE) ||
			(util::IsSame< T, std::vector<Value> >::VALUE) ||
			(util::IsSame< T, std::map<std::string, Value> >::VALUE));

	if (!is<T>(src)) {
		errorByType(typeToString<T>(), typeToString(src), path);
	}
}

template<typename T>
const T* JsonUtils::find(const picojson::value &src) {
	return (is<T>(src) ? &get<T>(src) : NULL);
}

template<typename T>
T* JsonUtils::find(picojson::value &src) {
	return (is<T>(src) ? &get<T>(src) : NULL);
}

template<typename T>
const T* JsonUtils::find(
		const picojson::value &src, const u8string &key, Path *path) {
	return findFromValue<const picojson::value, const T, T>(src, key, path);
}

template<typename T>
T* JsonUtils::find(
		picojson::value &src, const u8string &key, Path *path) {
	return findFromValue<picojson::value, T, T>(src, key, path);
}

template<typename T>
bool JsonUtils::findInt(const picojson::value &src, T *result, Path *path) {
	assert(result != NULL);
	*result = T();

	const double *value = find<double>(src);
	if (value == NULL) {
		return false;
	}

	*result = asInt<T>(*value, path);
	return true;
}

template<typename T>
bool JsonUtils::findInt(
		const picojson::value &src, const u8string &key, T *result,
		Path *path) {
	assert(result != NULL);
	*result = T();

	Path localPath = (path == NULL ? Path() : *path);
	Path *localPathAddr = (path == NULL ? NULL : &localPath);

	const double *value = find<double>(src, key, localPathAddr);
	if (value == NULL) {
		return false;
	}

	*result = asInt<T>(*value, localPathAddr);
	applyPath(&result, localPath, path);
	return true;
}

template<typename T>
const T& JsonUtils::as(const T &src, const Path*) {
	return src;
}

template<typename T>
T& JsonUtils::as(T &src, const Path*) {
	return src;
}

template<typename T>
const T* JsonUtils::find(const T &src) {
	return &src;
}

template<typename T>
T* JsonUtils::find(T &src) {
	return &src;
}

template<typename T>
bool JsonUtils::is(const picojson::value &value) {
	typedef typename util::IsSame<picojson::value, T>::Type SameType;
	return is<picojson::value, T>(value, SameType());
}

template<typename V, typename T>
bool JsonUtils::is(const V &value, const util::TrueType&) {
	static_cast<void>(value);
	return true;
}

template<typename V, typename T>
bool JsonUtils::is(const V &value, const util::FalseType&) {
	return value.template is<T>();
}

template<typename T>
const T& JsonUtils::get(const picojson::value &value) {
	typedef typename util::IsSame<picojson::value, T>::Type SameType;
	return get<const picojson::value, const T, T>(value, SameType());
}

template<typename T>
T& JsonUtils::get(picojson::value &value) {
	typedef typename util::IsSame<picojson::value, T>::Type SameType;
	return get<picojson::value, T, T>(value, SameType());
}

template<typename V, typename T, typename U>
T& JsonUtils::get(V &value, const util::TrueType&) {
	return value;
}

template<typename V, typename T, typename U>
T& JsonUtils::get(V &value, const util::FalseType&) {
	return value.template get<U>();
}

template<typename V, typename T, typename U>
T& JsonUtils::valueAs(V &src, const u8string &key, Path *path) {
	Path localPath = (path == NULL ? Path() : *path);
	Path *localPathAddr = (path == NULL ? NULL : &localPath);

	V &value = asValue<V, V>(src, key, localPathAddr);
	return *applyPath(&as<U>(value, localPathAddr), localPath, path);
}

template<typename V, typename T, typename U>
T* JsonUtils::findFromValue(V &src, const u8string &key, Path *path) {
	Path localPath = (path == NULL ? Path() : *path);
	V *value = findValue<V, V>(src, key, &localPath);
	return applyPath(
			(value == NULL ? NULL : find<U>(*value)), localPath, path);
}

template<typename T>
T* JsonUtils::applyPath(T *value, const Path &localPath, Path *path) {
	if (value != NULL && path != NULL) {
		*path = localPath;
	}
	return value;
}


#endif
